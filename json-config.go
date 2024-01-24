package main

import (
	"encoding/json"
	"log"
	"os"
)

type Config struct {
	WalSize           uint64 `json:"wal_size"`
	MemtableSize      uint64 `json:"memtable_size"`
	MemtableStructure string `json:"memtable_structure"`
	CacheCapacity	  uint64 `json:"cache_capacity"`
}

func config() (Config){
	var config Config
	configData, err := os.ReadFile("config.json")
	if err != nil {
		log.Fatal(err)
	}
	json.Unmarshal(configData, &config)
	/*
	fmt.Println(config.MemtableStructure)
	marshalled, err := json.Marshal(config)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(string(marshalled))
	*/

	return config
}
