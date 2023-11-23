package config

import "fmt"

// MRStruct for M and R values
type MRStruct struct {
	M int
	R int
}

// ShardSize is the predeterminded size of the shard.
var ShardSize int64 = 200000

// MapTask is a type of task
var MapTask string = "map"

// AccountName is the storage account in Azure.
var AccountName = "workshop7go"

// AccountKey is the account key
var AccountKey = "JQanrxg5u6ikek1LLvKFemkYO5UD/Grhlz8mDWdrcI9L2h9Y2KaYzyEEtj0a4Kpk3lLsjzHrKcV8+AStn3FyIA=="

// ContainerURL is the input file container with files that are to be sharded.
var ContainerURL = fmt.Sprintf("https://%s.blob.core.windows.net/dataset", AccountName)

// Worker states
// Idle state
var Idle = "idle"

// Busy state
var Busy = "busy"

// Down state
var Down = "down"

// Shard states
// Processed state
var Complete = "complete"

// Busy state
var InProcess = "In_process"

// Failed state
var Failed = "failed"

// Timed Out
var TimedOut = "timed_out"

var MRConfigSmall = MRStruct{
	M: 20,
	R: 5,
}

var MRConfigLarge = MRStruct{
	M: 100,
	R: 20,
}
