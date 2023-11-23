package main

type Worker struct {
	IP    string
	State string
	Port  string
}

type WorkerStateMap map[int]Worker

type ShardMap struct {
	Shards []Shard
}
