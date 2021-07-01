package main

import (
	"reflect"
)

type ConfigCollection struct {
	Collection string         `json:"collection"`
	CapSize    int            `json:"cap"`
	Index      []*ConfigIndex `json:"index"`
}

type ConfigIndex struct {
	Key                []map[string]int32 `json:"key"`
	Unique             bool               `json:"unique"`
	ExpireAfterSeconds *int32             `json:"expireAfterSeconds,omitempty"`
}

type IndexDiff struct {
	Old map[string]map[string]IndexModel
	New map[string]map[string]IndexModel
	Cap map[string]int
}

type IndexModel struct {
	Name               string             `json:"name"`
	Key                []map[string]int32 `json:"key"`
	Unique             bool               `json:"unique"`
	ExpireAfterSeconds *int32             `json:"expireAfterSeconds"`
}

func (m *IndexModel) Compare(index *ConfigIndex) bool {
	return reflect.DeepEqual(m.Key, index.Key) &&
		m.Unique == index.Unique &&
		reflect.DeepEqual(m.ExpireAfterSeconds, index.ExpireAfterSeconds)
}
