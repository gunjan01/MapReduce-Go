package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/workshop7/config"
	mapreducepb "github.com/workshop7/protos"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
)

func fetchWorkers() WorkerStateMap {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"localhost:2379"},
		DialTimeout: 10 * time.Second,
	})
	if err != nil {
		log.Fatal("Could not establish connection. Failed with err: ", err)
		os.Exit(1)
	}

	ctx := context.Background()

	/* Getting the worker pod ips for load balancing" */
	kv := clientv3.NewKV(cli)
	opts := []clientv3.OpOption{
		clientv3.WithPrefix(),
		clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend),
		clientv3.WithLimit(6),
	}

	gr, _ := kv.Get(ctx, "Worker", opts...)

	var workers []string
	for _, item := range gr.Kvs {
		fmt.Println(string(item.Key), string(item.Value))
		workers = append(workers, string(item.Value))
	}

	cli.Close()

	workerMap := make(map[int]Worker)

	for index, worker := range workers {
		workerMap[index] = Worker{
			IP:    worker,
			State: config.Idle,
			Port:  fmt.Sprintf("808%d", index),
		}
	}

	return workerMap
}

func fetchFollowers() []string {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"localhost:2379"},
		DialTimeout: 10 * time.Second,
	})
	if err != nil {
		log.Fatal("Could not establish connection. Failed with err: ", err)
		os.Exit(1)
	}

	ctx := context.Background()

	/* Getting the worker pod ips for load balancing" */
	kv := clientv3.NewKV(cli)
	opts := []clientv3.OpOption{
		clientv3.WithPrefix(),
		clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend),
		clientv3.WithLimit(6),
	}

	gr, _ := kv.Get(ctx, "Master", opts...)

	// Get all follower ips.
	var followers []string
	for _, item := range gr.Kvs {
		fmt.Println(string(item.Key), string(item.Value))
		followers = append(followers, string(item.Value))
	}

	cli.Close()

	return followers
}

func fetchIdleWorker(workermap WorkerStateMap) int {

	for index, _ := range workermap {
		M.RLock()
		if workermap[index].State == config.Idle {
			return index
		}
		M.RUnlock()
	}

	return -1
}

// SendRecomputeMapRequest sends recompute map request to followers.
func SendRecomputeMapRequest(jobID string) *mapreducepb.RecomputeRequest {
	request := mapreducepb.RecomputeRequest{
		JobId:    jobID,
		TaskType: "map",
	}

	return &request
}

// SendRecomputeMapRequest sends recompute reduce request to followers.
func SendRecomputeReduceRequest(jobID string, reduceMap map[int]string) *mapreducepb.RecomputeRequest {
	// Ugly typecast stuff !
	var reduceTaskMap = make(map[int32]string)
	for index, value := range reduceMap {
		reduceTaskMap[int32(index)] = value
	}

	request := &mapreducepb.RecomputeRequest{
		JobId:     jobID,
		TaskType:  "reduce",
		Reducemap: reduceTaskMap,
	}

	return request
}

// SendRecomputeRequestToAllFollowers sends recompute request to
// all followers for recomputing either map or reduce.
func SendRecomputeRequestToAllFollowers(request *mapreducepb.RecomputeRequest) {
	if request != nil {
		followers := fetchFollowers()

		for _, followerIP := range followers {
			if followerIP != os.Getenv("MASTER_IP") {
				var conn *grpc.ClientConn
				// Change here
				//address := followerIP + ":8000"
				address := ":" + followerIP

				log.Printf("IP is %s *****************", followerIP)

				// Dial a connection
				conn, err := grpc.Dial(address, grpc.WithTimeout(50*time.Second), grpc.WithInsecure())
				if err != nil {
					log.Fatalf("Could not connect to the follower at ip %s. Failed with err", followerIP)
				}
				defer conn.Close()

				// Context with deadlines to handle slow workers
				ctx, cancel := context.WithTimeout(context.Background(), (10 * time.Second))
				defer cancel()

				client := mapreducepb.NewProcessorClient(conn)
				response, err := client.Recompute(ctx, request)

				if response != nil {
					log.Println("Response received: ", response.Acknowlege)
				}
			}
		}
	}
}
