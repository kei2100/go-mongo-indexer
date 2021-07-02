package main

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"reflect"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/kei2100/go-mongo-indexer/v2/pkg/util"
)

const GB1 = 1000000000

// Execute the command
func execute() {

	indexDiff := getIndexesDiff()

	if !*apply {
		showDiff(indexDiff)
	}

	if *apply {
		applyDiff(indexDiff)
	}
}

// Drop and apply the indexes
func applyDiff(indexDiff *IndexDiff) {
	for _, collection := range Collections() {
		indexesToRemove := indexDiff.Old[collection]
		indexesToAdd := indexDiff.New[collection]
		capToAdd := indexDiff.Cap[collection]

		util.PrintBold(fmt.Sprintf("\n%s.%s\n", db.Name(), collection))

		if indexesToRemove == nil && indexesToAdd == nil && capToAdd == 0 {
			util.PrintGreen(fmt.Sprintln("No index changes"))
			continue
		}

		if capToAdd != 0 {
			util.PrintGreen(fmt.Sprintf("+ Adding cap of %d\n", capToAdd))
			SetCapSize(collection, capToAdd)
		}

		for _, index := range indexesToRemove {
			util.PrintRed(fmt.Sprintf("- Dropping index %s: %s\n", index.Name, util.JsonEncode(index)))
			DropIndex(collection, index.Name)
		}

		for _, index := range indexesToAdd {
			util.PrintGreen(fmt.Sprintf("+ Adding index %s: %s\n", index.Name, util.JsonEncode(index)))
			CreateIndex(collection, index.Name, index)
		}
	}
}

// Create index of on the given collection with index Name and columns
func CreateIndex(collection string, indexName string, indexModel IndexModel) bool {
	var keys bson.D
	for _, m := range indexModel.Key {
		for k, v := range m {
			keys = append(keys, bson.E{Key: k, Value: v})
		}
	}
	background := true

	// setting options
	opts := &options.IndexOptions{
		Unique:             &indexModel.Unique,
		Background:         &background,
		Name:               &indexName,
		ExpireAfterSeconds: indexModel.ExpireAfterSeconds,
		Sparse:             &indexModel.Sparse,
	}

	index := mongo.IndexModel{
		Keys:    keys,
		Options: opts,
	}

	indexView := db.Collection(collection).Indexes()

	_, err := indexView.CreateOne(context.TODO(), index)

	if err != nil {
		log.Fatalf("create index: %+v", err.Error())
	}

	return true
}

// Drop an index by Name from given collection
func DropIndex(collection string, indexName string) bool {
	indexes := db.Collection(collection).Indexes()
	_, err := indexes.DropOne(context.TODO(), indexName)

	if err != nil {
		log.Fatalf("drop index: %+v", err.Error())
	}

	return true
}

// Show the index difference, the indexes with `-` will be deleted only
// the ones with the `+` will be created
func showDiff(indexDiff *IndexDiff) {

	for _, collection := range Collections() {
		indexesToRemove := indexDiff.Old[collection]
		indexesToAdd := indexDiff.New[collection]
		capToAdd := indexDiff.Cap[collection]

		util.PrintBold(fmt.Sprintf("\n%s.%s\n", db.Name(), collection))

		if indexesToRemove == nil && indexesToAdd == nil && capToAdd == 0 {
			util.PrintGreen(fmt.Sprintln("No index changes"))
			continue
		}

		if capToAdd != 0 {
			util.PrintGreen(fmt.Sprintf("+ Capsize to set: %d\n", capToAdd))
		}

		for _, index := range indexesToRemove {
			util.PrintRed(fmt.Sprintf("- %s: %s\n", index.Name, util.JsonEncode(index)))
		}

		for _, index := range indexesToAdd {
			util.PrintGreen(fmt.Sprintf("+ %s: %s\n", index.Name, util.JsonEncode(index)))
		}
	}
}

// Match existing indexes with the given config file and match and find the diff
// the indexes that are not inside the config will be deleted, only the indexes in
// the config file will be created
func getIndexesDiff() *IndexDiff {

	oldIndexes := make(map[string]map[string]IndexModel)
	newIndexes := make(map[string]map[string]IndexModel)
	capSize := make(map[string]int)

	for _, collection := range Collections() {

		var alreadyAppliedIndexes []IndexModel
		var alreadyAppliedIndexesNames []string
		var givenIndexes []*ConfigIndex

		configCollection := GetConfigCollection(collection)

		if configCollection != nil {
			givenIndexes = configCollection.Index
		}

		// Get current database collection indexes
		currentIndexes := DbIndexes(collection)

		// If we don't have the current collection in the index create list then drop all index
		if !IsCollectionToIndex(collection) {
			for _, dbIndex := range currentIndexes {
				if oldIndexes[collection] == nil {
					oldIndexes[collection] = make(map[string]IndexModel)
				}
				oldIndexes[collection][dbIndex.Name] = dbIndex
			}
			continue
		}

		// Get the config cap size
		givenCapSize := GetConfigCollectionCapSize(collection)
		isAlreadyCapped := IsCollectionCaped(collection)

		minAllowedCapSize := GB1 / 2

		// Add the cap size
		if givenCapSize > 0 && (givenCapSize >= minAllowedCapSize) && !isAlreadyCapped {
			capSize[collection] = givenCapSize
		}

		// Prepare the list of indexes that need to be dropped
		for _, dbIndex := range currentIndexes {

			isCurrentIndexInConfig := false

			for _, givenIndex := range givenIndexes {

				// If the Name of index matches the Name of given index
				generatedIndexName := GenerateIndexName(givenIndex)

				if dbIndex.Name == generatedIndexName {
					isCurrentIndexInConfig = true
					alreadyAppliedIndexesNames = append(alreadyAppliedIndexesNames, generatedIndexName)
					break
				}

				// First check if this column group has the index
				if dbIndex.Compare(givenIndex) {
					isCurrentIndexInConfig = true
					break
				}
			}

			if !isCurrentIndexInConfig {
				if oldIndexes[collection] == nil {
					oldIndexes[collection] = make(map[string]IndexModel)
				}
				oldIndexes[collection][dbIndex.Name] = dbIndex
			} else {
				alreadyAppliedIndexes = append(alreadyAppliedIndexes, dbIndex)
			}
		}

		// For each of the given indexes, check if it is already applied or not
		// If not, prepare a list so that those can be applied
		for _, givenIndex := range givenIndexes {

			isAlreadyApplied := false

			// If the Name of index matches the Name of given index
			generatedIndexName := GenerateIndexName(givenIndex)

			for _, appliedIndex := range alreadyAppliedIndexes {

				if util.StringInSlice(generatedIndexName, alreadyAppliedIndexesNames) {
					isAlreadyApplied = true
					break
				}

				if appliedIndex.Compare(givenIndex) {
					isAlreadyApplied = true
					break
				}
			}

			if !isAlreadyApplied {
				if newIndexes[collection] == nil {
					newIndexes[collection] = make(map[string]IndexModel)
				}
				newIndexes[collection][generatedIndexName] = IndexModel{
					Name:               generatedIndexName,
					Key:                givenIndex.Key,
					Unique:             givenIndex.Unique,
					ExpireAfterSeconds: givenIndex.ExpireAfterSeconds,
					Sparse:             givenIndex.Sparse,
				}
			}
		}
	}

	return &IndexDiff{oldIndexes, newIndexes, capSize}
}

// Generate index Name by doing md5 of indexes json
func GenerateIndexName(indexColumns interface{}) string {
	content, _ := json.Marshal(indexColumns)
	algorithm := md5.New()
	algorithm.Write(content)

	return hex.EncodeToString(algorithm.Sum(nil))
}

// Return list of config collections
func Collections() []string {
	var cols []string
	for _, cc := range ConfigCollections() {
		cols = append(cols, cc.Collection)
	}
	return cols
}

// Return database collection indexes
func DbIndexes(collection string) []IndexModel {
	cursor, err := db.Collection(collection).Indexes().List(context.TODO())

	if err != nil {
		log.Fatalln(err.Error())
	}

	dbIndexes := make([]IndexModel, 0)

	for cursor.Next(context.TODO()) {
		src := bson.M{}
		var dst IndexModel

		if err := cursor.Decode(&src); err != nil {
			log.Fatalln(err.Error())
		}

		var keys bson.D
		keysByte, _ := bson.Marshal(src["key"])
		if err := bson.Unmarshal(keysByte, &keys); err != nil {
			log.Fatalf("unmarshal bson key: %v", err)
		}
		// ignore the _id index as it's the default index
		if len(keys) == 0 || reflect.DeepEqual(keys, bson.D{{Key: "_id", Value: int32(1)}}) {
			continue
		}
		for _, k := range keys {
			dst.Key = append(dst.Key, map[string]int32{
				k.Key: k.Value.(int32),
			})
		}

		// check if there's a unique index or not
		unique, exists := src["unique"]
		if exists {
			dst.Unique = unique.(bool)
		}

		// check if there's a unique index or not
		expireAfterSeconds, exists := src["expireAfterSeconds"]
		if exists {
			v := expireAfterSeconds.(int32)
			dst.ExpireAfterSeconds = &v
		}

		// check if there's a sparse index or not
		sparse, exists := src["sparse"]
		if exists {
			dst.Sparse = sparse.(bool)
		}

		dst.Name = src["name"].(string)

		dbIndexes = append(dbIndexes, dst)
	}

	return dbIndexes
}

// Drop index from collection by index Name
func IsCollectionToIndex(collection string) bool {
	return GetConfigCollection(collection) != nil
}

// Check if the collection is already capped
func IsCollectionCaped(collection string) bool {
	if !IsDatabaseExists() {
		return false
	}
	if !IsCollectionExists(collection) {
		return false
	}

	command := map[string]string{"collStats": collection}
	result := db.RunCommand(context.TODO(), command)

	var doc bson.M
	if err := result.Decode(&doc); err != nil {
		log.Fatalf("get collStats: %v", err.Error())
	}

	return doc["capped"].(bool)
}

// Check if tha database exists
func IsDatabaseExists() bool {
	result, err := db.Client().ListDatabaseNames(context.TODO(), bson.M{"name": *database})
	if err != nil {
		log.Fatalf("list database names: %v", err.Error())
	}
	return len(result) > 0
}

func IsCollectionExists(collection string) bool {
	result, err := db.ListCollectionNames(context.TODO(), bson.M{"name": collection})
	if err != nil {
		log.Fatalf("list collection names: %v", err.Error())
	}
	return len(result) > 0
}

func SetCapSize(collection string, size int) bool {

	command := map[string]interface{}{"convertToCapped": collection, "size": size}
	result := db.RunCommand(context.TODO(), command)

	var doc bson.M
	if err := result.Decode(&doc); err != nil {
		log.Fatalln(err.Error())
	}

	return true
}
