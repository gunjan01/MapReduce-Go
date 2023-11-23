package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math"

	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/gorilla/mux"
	"github.com/workshop7/config"
	mapreducepb "github.com/workshop7/protos"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"google.golang.org/grpc"
)

type Job struct {
	ID   string `json:"int"`
	Type string `json:"type"`
}

var jobs chan Job
var RecomputJobs []*mapreducepb.RecomputeRequest

var workermap WorkerStateMap
var M sync.RWMutex

var mapTaskStatusMap map[int]string
var shardPerWorkerMap map[int][]Shard
var shards map[int][]Shard
var reducerMap map[int]string
var reduceTaskMap map[int][]string
var reducerTaskStatusMap map[int]string
var failedParameter int
var taskCounter int
var currentJobID string
var e *concurrency.Election

func init() {
	// To be removed.
	///os.Setenv("MASTER_IP", "localhost")
	//os.Setenv("FAILED_PARAMETER", "1")
	os.Setenv("MASTER_IP", os.Args[2])

	/*failParam, err := strconv.Atoi(os.Getenv("FAILED_PARAMETER"))
	if err == nil {
		failedParameter = failParam
	}
	log.Print("Failed Parameter is: ", failedParameter)*/

	failParam, err := strconv.Atoi(os.Args[3])
	if err == nil {
		failedParameter = failParam
	}
	log.Print("Failed Parameter is: ", failedParameter)

	jobs = make(chan Job, 100)
	workermap = make(map[int]Worker)
	shardPerWorkerMap = make(map[int][]Shard)
	reduceTaskMap = make(map[int][]string)
	mapTaskStatusMap = make(map[int]string)
	reducerMap = make(map[int]string)
	reducerTaskStatusMap = make(map[int]string)

	// Put ip in etcd
	var cli *clientv3.Client
	cli, err = clientv3.New(clientv3.Config{
		Endpoints:   []string{"localhost:2379"},
		DialTimeout: 10 * time.Second,
	})
	if err != nil {
		log.Print("Could not establish connection. Failed with err: ", err)
	}
	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	kv := clientv3.NewKV(cli)
	key := "Master_" + randomString()
	kv.Put(ctx, key, os.Getenv("MASTER_IP"))
	cli.Close()
}

// randomString generates a randome string
func randomString() string {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	return strconv.Itoa(r.Int())
}

func sendHeartbeat(workerId int) {
	var conn *grpc.ClientConn

	ip := workermap[workerId].IP
	address := ":" + workermap[workerId].Port

	conn, err := grpc.Dial(address, grpc.WithTimeout(50*time.Second), grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Could not connect to the server at port %s. Failed with err", ip)
	}
	defer conn.Close()

	// Context with deadlines to handle slow workers
	ctx, cancel := context.WithTimeout(context.Background(), (5 * time.Second))
	defer cancel()

	client := mapreducepb.NewProcessorClient(conn)
	response, err := client.SayHello(ctx, &mapreducepb.PingMessage{
		Greeting: "Request heartbeat",
	})

	// An error can happen if the worker is down or if the worker is slow
	// In the case of slow, the context just cancels.
	if err != nil {
		log.Println("Error while checking heartbeat", err)

		// Set status as down.
		M.RLock()
		worker := workermap[workerId]
		worker.State = config.Down
		workermap[workerId] = worker

		log.Printf("Setting down status")
		log.Printf("New  worker map %+v", workermap)

		M.RUnlock()
	}

	if response != nil {
		log.Printf("Worker %d is doing fine. Response: %s", workerId, response.Greeting)
	}

}

func sendMapRequest(workerId int, taskID int, waitGroup *sync.WaitGroup) {
	if failedParameter != 0 {
		if taskCounter == failedParameter {
			SendRecomputeRequestToAllFollowers(SendRecomputeMapRequest(currentJobID))

			if err := e.Resign(context.Background()); err != nil {
				log.Fatal(err)
			}
			os.Exit(1)
		}
	}

	var conn *grpc.ClientConn
	ip := workermap[workerId].IP
	address := ":" + workermap[workerId].Port
	shardsToBeProcessed := shardPerWorkerMap[taskID]

	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Could not connect to the server at port %s. Failed with err", ip)
	}
	defer conn.Close()

	client := mapreducepb.NewProcessorClient(conn)

	grpcShards := []*mapreducepb.Shard{}
	for _, shard := range shardsToBeProcessed {
		grpcShard := mapreducepb.Shard{
			FileName: shard.Filename,
			Start:    shard.Start,
			End:      shard.End,
		}

		grpcShards = append(grpcShards, &grpcShard)
	}

	log.Printf("Worker ID %d is processign map task ID %d", workerId, taskID)

	response, err := client.AssignTask(context.Background(), &mapreducepb.TaskRequest{
		FileShard: grpcShards,
		Type:      "map",
		MapTaskId: int32(taskID),
	})

	if response != nil {
		if response.TaskStatus == config.Complete {
			M.RLock()
			// set worker back as idle
			worker := workermap[workerId]
			worker.State = config.Idle
			workermap[workerId] = worker

			mapTaskStatusMap[taskID] = config.Complete

			reducerMap[taskID] = response.TempFiles
			M.RUnlock()

			log.Printf("Response from server: %s", response.TaskStatus)
			log.Printf("Response from server: %s", response.TempFiles)
		}
	}

	if err != nil {
		log.Printf("Call to grpc server failed with error: %s", err)

		M.RLock()
		worker := workermap[workerId]
		worker.State = config.Down
		workermap[workerId] = worker

		mapTaskStatusMap[taskID] = config.Failed
		M.RUnlock()
	}

	if waitGroup != nil {
		waitGroup.Done()
	}

	taskCounter = taskCounter + 1
}

func sendReduceRequest(workerId int, taskID int, waitGroup *sync.WaitGroup) {
	if failedParameter != 0 {
		if taskCounter == failedParameter {
			SendRecomputeRequestToAllFollowers(SendRecomputeReduceRequest(currentJobID, reducerMap))
			log.Print("Resign ****************")
			if err := e.Resign(context.Background()); err != nil {
				log.Fatal(err)
			}
			os.Exit(1)
		}
	}

	var conn *grpc.ClientConn

	ip := workermap[workerId].IP
	address := ":" + workermap[workerId].Port

	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Could not connect to the server at port %s. Failed with err", ip)
	}
	defer conn.Close()

	client := mapreducepb.NewProcessorClient(conn)

	response, err := client.AssignTask(context.Background(), &mapreducepb.TaskRequest{
		IntermediateFilePath: reduceTaskMap[taskID],
		Type:                 "reduce",
	})

	if response != nil {
		if response.TaskStatus == config.Complete {
			M.RLock()
			worker := workermap[workerId]
			worker.State = config.Idle
			workermap[workerId] = worker

			reducerTaskStatusMap[taskID] = config.Complete
			M.RUnlock()

			log.Printf("Response from server: %s", response.TaskStatus)
		}
	}

	if err != nil {
		log.Printf("Call to grpc server failed with error: %s", err)

		M.RLock()
		worker := workermap[workerId]
		worker.State = config.Down
		workermap[workerId] = worker

		reducerTaskStatusMap[taskID] = config.Failed
		M.RUnlock()
	}

	if waitGroup != nil {
		waitGroup.Done()
	}

	taskCounter = taskCounter + 1
}

func checkOnWorkers() {
	for true {
		log.Println("I am going to handle heartbeat")
		log.Printf("Available workers are:  %+v", workermap)
		for index, worker := range workermap {
			// Only ping workers that are up.
			if worker.State != config.Down {
				log.Printf("Healthcheck for worker ID: %d", index)
				sendHeartbeat(index)
			}
		}
		time.Sleep(10 * time.Second)
	}
}

func main() {
	signalChan := make(chan os.Signal, 100)
	signal.Notify(signalChan, syscall.SIGABRT, syscall.SIGKILL,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT,
	)

	var wg sync.WaitGroup
	wg.Add(3)

	var cli *clientv3.Client
	//var e *concurrency.Election
	var err error

	ctx := context.Background()

	// Fetch registered workers
	// in the system and set their state
	// as idle.
	workermap = fetchWorkers()

	go func() {
		defer wg.Done()
		for true {
			select {
			case <-signalChan: // first signal, cancel context
				log.Print("Exit signal received. Resigning")

				if err = e.Resign(ctx); err != nil {
					log.Fatal(err)
				}
				cli.Close()
				os.Exit(1)
			}
		}
	}()

	go func() {
		defer wg.Done()

		go func() {
			RecomputeServer()
		}()

		// Create a etcd client
		log.Print("Connecting to etcd")
		cli, err = clientv3.New(clientv3.Config{
			Endpoints:   []string{"localhost:2379"},
			DialTimeout: 10 * time.Second,
		})
		if err != nil {
			log.Fatal("Could not establish connection. Failed with err: ", err)
			os.Exit(1)
		}
		defer cli.Close()

		// create a sessions to elect a Leader
		s, err := concurrency.NewSession(cli)
		if err != nil {
			log.Fatal(err)
			os.Exit(1)
		}
		defer s.Close()

		e = concurrency.NewElection(s, "/leader-election/")
		log.Print("Established cli connection")

		// Elect a leader (or wait that the leader resign)
		if err := e.Campaign(ctx, "e"); err != nil {
			log.Fatal("Electing a leader failed with err")
			os.Exit(1)
		}

		fmt.Println("leader election done ")

		// Put ip in etcd
		ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
		kv := clientv3.NewKV(cli)
		kv.Put(ctx, "Leader", os.Getenv("MASTER_IP"))

		// Check for existing jobs
		/*opts := []clientv3.OpOption{
			clientv3.WithPrefix(),
			clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend),
			clientv3.WithLimit(6),
		}

		gr, _ := kv.Get(ctx, "Job", opts...)

		for _, item := range gr.Kvs {
			fmt.Println(string(item.Key), string(item.Value))
			if string(item.Value) == config.InProcess {
				log.Println("Found a job to be processed with id: ", string(item.Key)[4:])

				jobs <- Job{
					ID:   string(item.Key)[4:],
					Type: "map-reduce",
				}
			}
		}*/

		for _, request := range RecomputJobs {

			log.Print(" ************************* Processing **************")

			if request != nil {
				if request.TaskType == "map" {
					jobs <- Job{
						ID: request.JobId,
					}
				}

				// Only process reduce.
				if request.TaskType == "reduce" {
					go processReduce(request)
				}
			}
		}

		// Go routine to check on workers.
		go checkOnWorkers()

		for true {
			// If there are jobs in the job queue
			if len(jobs) > 0 {
				job := <-jobs

				currentJobID = job.ID

				log.Printf("Got a job request. Starting to process job with id: %s", job.ID)

				// Put the job in etcd
				key := "Job_" + job.ID
				kv.Put(context.Background(), key, config.InProcess)

				// Fetch workers which are available.
				//workermap = fetchWorkers()

				// There must be workers available to process a job
				if len(workermap) > 0 {
					log.Print("Sharding request to the worker")

					shards := ShardFiles()

					fmt.Printf("Number of shards : %d", len(shards))
					fmt.Printf("Number of shards per map task: %d", len(shards)/config.MRConfigSmall.M)

					// Divide the shards per worker
					// If there are 10 shards and 3 workers, each worker processes 3 shards and one processes 4.
					taskCount := 0
					for i := 0; i < config.MRConfigSmall.M; i++ {
						for j := 0; j < int(math.Ceil(float64(len(shards))/float64(config.MRConfigSmall.M))); j++ {
							if taskCount < len(shards) {
								shardPerWorkerMap[i] = append(shardPerWorkerMap[i], shards[taskCount]...)
								taskCount += 1
							}
						}
					}

					fmt.Println("Worker to shard map constructed is:", shardPerWorkerMap)

					fmt.Println("Number of map tasks are:", len(shardPerWorkerMap))

					map_task_id := 0
					var waitGroupMapTask sync.WaitGroup

					// Assign the first round of map tasks
					for map_task_id != len(shardPerWorkerMap) {
						workerID := fetchIdleWorker(workermap)

						if workerID != -1 {
							waitGroupMapTask.Add(1)

							// Mark worker busy and shard in process
							M.RLock()
							worker := workermap[workerID]
							worker.State = config.Busy
							workermap[workerID] = worker

							mapTaskStatusMap[map_task_id] = config.InProcess
							M.RUnlock()

							// Send map request
							go sendMapRequest(workerID, map_task_id, &waitGroupMapTask)

							// Increment map task
							map_task_id = map_task_id + 1
						}
						time.Sleep(2 * time.Second)
					}

					waitGroupMapTask.Wait()

					count := 0
					done := false

					// Process map tasks that failed
					for done != true {
						for i := 0; i < len(mapTaskStatusMap); i++ {
							if count == len(mapTaskStatusMap) {
								done = true
								break
							}

							if mapTaskStatusMap[i] == config.Complete {
								M.RLock()
								mapTaskStatusMap[i] = "MARK_COMPLETE"
								M.RUnlock()
								count = count + 1
							}

							if mapTaskStatusMap[i] == config.Failed || mapTaskStatusMap[i] == config.TimedOut {
								var is_done bool = false

								for is_done != true {
									workerID := fetchIdleWorker(workermap)

									if workerID != -1 {
										// Mark worker busy and shard in process
										M.RLock()
										worker := workermap[workerID]
										worker.State = config.Busy
										workermap[workerID] = worker

										mapTaskStatusMap[i] = config.InProcess
										M.RUnlock()

										// Send map request
										sendMapRequest(workerID, i, nil)

										is_done = true
									}

									time.Sleep(2 * time.Second)
								}
							}
						}
					}

					log.Println("All map tasks done. Ready to start reduce.")
					time.Sleep(5 * time.Second)

					// Divide the shards per worker
					// If there are 10 shards and 3 workers, each worker processes 3 shards and one processes 4.
					taskCount = 0

					for i := 0; i < config.MRConfigSmall.R; i++ {
						for j := 0; j < int(math.Ceil(float64(len(reducerMap))/float64(config.MRConfigSmall.R))); j++ {
							if taskCount < len(reducerMap) {
								reduceTaskMap[i] = append(reduceTaskMap[i], reducerMap[taskCount])
								taskCount += 1
							}
						}
					}

					log.Printf("Reducer task map is %+v", reduceTaskMap)

					var waitGroupReduceTask sync.WaitGroup
					var reducer_task_id = 0

					// Assign the first round of reduce tasks
					for reducer_task_id != len(reduceTaskMap) {
						workerID := fetchIdleWorker(workermap)

						if workerID != -1 {
							waitGroupReduceTask.Add(1)

							// Mark worker busy and shard in process
							M.RLock()
							worker := workermap[workerID]
							worker.State = config.Busy
							workermap[workerID] = worker

							reducerTaskStatusMap[reducer_task_id] = config.InProcess
							M.RUnlock()

							// Send map request
							go sendReduceRequest(workerID, reducer_task_id, &waitGroupReduceTask)

							// Increment map task
							reducer_task_id = reducer_task_id + 1
						} else {
							log.Println("No idle worker found. Waiting for one.")
						}

						time.Sleep(2 * time.Second)
					}

					waitGroupReduceTask.Wait()

					var reducerCount int = 0
					allReducersDone := false

					log.Printf("Reduce map is: %+v", reducerTaskStatusMap)
					// Process map tasks that failed
					for allReducersDone != true {
						for i := 0; i < len(reduceTaskMap); i++ {
							if reducerCount == len(reduceTaskMap) {
								allReducersDone = true

								log.Println("Breaking off loop")
								break
							}

							if reducerTaskStatusMap[i] == config.Complete {
								M.RLock()
								reducerTaskStatusMap[i] = "MARK_COMPLETE"
								M.RUnlock()
								reducerCount = reducerCount + 1
							}

							if reducerTaskStatusMap[i] == config.Failed || reducerTaskStatusMap[i] == config.TimedOut {
								var is_done bool = false

								for is_done != true {
									workerID := fetchIdleWorker(workermap)

									if workerID != -1 {
										// Mark worker busy and shard in process
										M.RLock()
										worker := workermap[workerID]
										worker.State = config.Busy
										workermap[workerID] = worker

										reducerTaskStatusMap[i] = config.InProcess
										M.RUnlock()

										// Send map request
										sendReduceRequest(workerID, i, nil)

										is_done = true
									} else {
										log.Println("No idle worker found. Waiting for one.")
									}

									time.Sleep(2 * time.Second)
								}
							}
						}
					}

					log.Print("Reducer task is complete.")
					log.Println("Job processed successfully.")

					// Update job
					kv.Put(context.Background(), key, config.Complete)
				}
			}
		}
	}()

	go func() {
		defer wg.Done()

		routes := mux.NewRouter()
		routes.HandleFunc("/job/submit", func(w http.ResponseWriter, r *http.Request) {
			decoder := json.NewDecoder(r.Body)

			log.Print("Received http request for job submission")

			var job Job
			err := decoder.Decode(&job)
			if err != nil {
				log.Fatalf("Unable to decode the request")
			}

			log.Printf("Successfully decoded job of type: %s", job.Type)

			job.ID = randomString()

			jobs <- job
		}).Methods("POST")

		log.Println("Starting http server on port 80")
		err := http.ListenAndServe(":"+os.Args[1], routes)
		if err != nil {
			log.Fatalf("Unable to start the server at port 80")
		}
	}()

	wg.Wait()

}
