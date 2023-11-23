package main

import (
	"context"
	"fmt"
	"log"
	"math"
	"net"
	"os"
	"sync"
	"time"

	"github.com/workshop7/config"
	mapreducepb "github.com/workshop7/protos"
	"google.golang.org/grpc"
)

var taskMap = make(map[int][]string)
var taskStatusMap = make(map[int]string)

// Server represents the gRPC server
type Server struct {
	mapreducepb.UnimplementedProcessorServer
}

func sendRequest(taskWaitGroup *sync.WaitGroup, task_id, workerID int) {
	var conn *grpc.ClientConn

	ip := workermap[workerID].IP
	address := ":" + workermap[workerID].Port

	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Could not connect to the server at port %s. Failed with err", ip)
	}
	defer conn.Close()

	client := mapreducepb.NewProcessorClient(conn)

	response, err := client.AssignTask(context.Background(), &mapreducepb.TaskRequest{
		IntermediateFilePath: taskMap[task_id],
		Type:                 "reduce",
	})

	if response != nil {
		if response.TaskStatus == config.Complete {
			M.RLock()
			worker := workermap[workerID]
			worker.State = config.Idle
			workermap[workerID] = worker

			taskStatusMap[task_id] = config.Complete
			M.RUnlock()

			log.Printf("Response from server: %s", response.TaskStatus)
		}
	}

	if err != nil {
		log.Printf("Call to grpc server failed with error: %s", err)

		M.RLock()
		worker := workermap[workerID]
		worker.State = config.Down
		workermap[workerID] = worker

		taskStatusMap[task_id] = config.Failed
		M.RUnlock()
	}

	taskWaitGroup.Done()

}

func processReduce(request *mapreducepb.RecomputeRequest) {
	// Divide the shards per worker
	// If there are 10 shards and 3 workers, each worker processes 3 shards and one processes 4.
	taskCount := 0

	log.Printf("Reduce map is: ************************** %+v", request.Reducemap)

	for i := 0; i < config.MRConfigSmall.R; i++ {
		for j := 0; j < int(math.Ceil(float64(len(request.Reducemap))/float64(config.MRConfigSmall.R))); j++ {
			if taskCount < len(request.Reducemap) {
				taskMap[i] = append(taskMap[i], request.Reducemap[int32(taskCount)])
				taskCount += 1
			}
		}
	}

	log.Printf("Reducer task map is %+v", taskMap)

	var taskWaitGroup sync.WaitGroup
	var task_id = 0

	// Assign the first round of reduce tasks
	for task_id != len(taskMap) {
		workerID := fetchIdleWorker(workermap)

		if workerID != -1 {
			taskWaitGroup.Add(1)

			// Mark worker busy and shard in process
			M.RLock()
			worker := workermap[workerID]
			worker.State = config.Busy
			workermap[workerID] = worker

			taskStatusMap[task_id] = config.InProcess
			M.RUnlock()

			taskWaitGroup.Add(1)

			go sendRequest(&taskWaitGroup, task_id, workerID)

			// Increment map task
			task_id = task_id + 1
		} else {
			log.Println("No idle worker found. Waiting for one.")
		}

		time.Sleep(2 * time.Second)
	}

	taskWaitGroup.Wait()

	var count int = 0
	done := false

	log.Printf("Reduce map is: %+v", taskStatusMap)
	// Process map tasks that failed
	for done != true {
		for i := 0; i < len(taskMap); i++ {
			if count == len(taskMap) {
				done = true

				log.Println("Breaking off loop")
				break
			}

			if taskStatusMap[i] == config.Complete {
				M.RLock()
				reducerTaskStatusMap[i] = "MARK_COMPLETE"
				M.RUnlock()
				count = count + 1
			}

			if taskStatusMap[i] == config.Failed || taskStatusMap[i] == config.TimedOut {
				var is_done bool = false

				for is_done != true {
					workerID := fetchIdleWorker(workermap)

					if workerID != -1 {
						// Mark worker busy and shard in process
						M.RLock()
						worker := workermap[workerID]
						worker.State = config.Busy
						workermap[workerID] = worker

						taskStatusMap[i] = config.InProcess
						M.RUnlock()

						sendRequest(nil, i, workerID)

						is_done = true
					} else {
						log.Println("No idle worker found. Waiting for one.")
					}

					time.Sleep(2 * time.Second)
				}
			}
		}
	}
}

// A Simple GRPC rpc ping call to check worker status
func (s *Server) Recompute(ctx context.Context, request *mapreducepb.RecomputeRequest) (*mapreducepb.RecomputResponse, error) {

	// Context cancelled
	if ctx.Err() == context.Canceled {
		log.Println("Client done waiting")
		return nil, ctx.Err()
	}

	/*// Add it to the list of jobs and reprocess everything.
	if request.TaskType == "map" {
		jobs <- Job{
			ID: request.JobId,
		}
	}

	// Only process reduce.
	if request.TaskType == "reduce" {
		go processReduce(request)
	}*/

	RecomputJobs = append(RecomputJobs, request)

	return &mapreducepb.RecomputResponse{Acknowlege: "acknowledged"}, nil
}

func RecomputeServer() {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%s", os.Args[2]))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	// create a server instance
	s := Server{}
	grpcServer := grpc.NewServer()

	mapreducepb.RegisterProcessorServer(grpcServer, &s)

	// start the server
	log.Printf("grpc server listening at %v", lis.Addr())

	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %s", err)
	}
}
