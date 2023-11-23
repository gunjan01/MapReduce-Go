package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/streaming"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/workshop7/client"
	"github.com/workshop7/config"
	mapreducepb "github.com/workshop7/protos"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
)

var port int
var key string
var failedParameter int
var taskCounter int
var mapID int

func init() {
	taskCounter = 1
	/*failParam, err := strconv.Atoi(os.Getenv("FAILED_PARAMETER"))
	if err == nil {
		failedParameter = failParam
	}

	log.Print("Failed Parameter is: ", failedParameter)*/
}

// Server represents the gRPC server
type Server struct {
	mapreducepb.UnimplementedProcessorServer
}

func randomString() string {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	return strconv.Itoa(r.Int())
}

func randomInt() int {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	return r.Int()
}

func createKeyValuePairs(m map[string]float64) string {
	b := new(bytes.Buffer)
	for key, value := range m {
		fmt.Fprintf(b, "%s : %g\n", key, value)
	}
	return b.String()
}

func processShard(index int, mapTaskID int, shard mapreducepb.Shard, wg *sync.WaitGroup) {
	containerClient := client.GetContainerClient(config.ContainerURL)

	blobClient := containerClient.NewBlockBlobClient(shard.FileName)
	offset := int64(shard.Start)
	count := int64(shard.End - shard.Start)

	get, err := blobClient.Download(context.TODO(), &azblob.DownloadBlobOptions{Offset: &offset, Count: &count})
	if err != nil {
		log.Println("Error while downloading")
		wg.Done()
		return
	}

	// Wrap the response body in a ResponseBodyProgress and pass a callback function for progress reporting.
	responseBody := streaming.NewResponseProgress(
		get.Body(nil),
		func(bytesTransferred int64) {
			//fmt.Printf("Read %d of %d bytes.", bytesTransferred, *get.ContentLength)
		},
	)

	downloadedData := &bytes.Buffer{}
	_, err = downloadedData.ReadFrom(responseBody)
	if err != nil {
		log.Print(err)
		wg.Done()
		return
	}

	err = responseBody.Close()
	if err != nil {
		log.Print(err)
		wg.Done()
		return
	}

	writeString := downloadedData.String()

	name := fmt.Sprintf("./intermediate/map%d", mapTaskID)
	err = os.MkdirAll(name, os.ModePerm)
	if err != nil {
		fmt.Println("Failed to create directory.Error: ", err)
		return
	}

	intermediateFile := fmt.Sprintf("./intermediate/map%d/shard_%d.txt", mapTaskID, index)
	f, err := os.Create(intermediateFile)
	if err != nil {
		log.Fatalf("Unable to create a file. Failed with error: %s", err.Error())
	}

	_, err = f.WriteString(writeString)
	if err != nil {
		log.Fatal("Unable to write to the file (intermediate directory map phase)")
	}
	//fmt.Printf("wrote %d bytes\n", numberOfBytesWritten)
	f.Close()

	var output []byte
	c := exec.Command("python3", "./scripts/mapper.py", intermediateFile)
	if output, err = c.Output(); err != nil {
		fmt.Println("Error: ", err)
		return
	}

	/*if err := os.Truncate(intermediateFile, 0); err != nil {
		log.Printf("Failed to truncate: %v", err)
	}

	f, err = os.OpenFile(intermediateFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("Unable to create a file. Failed with error: %s", err.Error())
	}

	_, err = f.WriteString(string(output))
	if err != nil {
		log.Fatal("Unable to write to the file: ", fmt.Sprintf("/tmp/shard_%d.txt", mapID))
	}
	f.Close()*/

	containerURLReduce := fmt.Sprintf("https://%s.blob.core.windows.net/intermediate/map%02d", config.AccountName, mapTaskID)
	containerClientReduce := client.GetContainerClient(containerURLReduce)
	blobClient = containerClientReduce.NewBlockBlobClient(fmt.Sprintf("map_%d.txt", index))

	// requestBody is the stream of data to write
	requestBody := streaming.NopCloser(strings.NewReader(string(output)))
	// Wrap the request body in a RequestBodyProgress and pass a callback function for progress reporting.
	requestProgress := streaming.NewRequestProgress(streaming.NopCloser(requestBody), func(bytesTransferred int64) {
		fmt.Printf("Wrote %d of %d bytes.", bytesTransferred, requestBody)
	})
	_, err = blobClient.Upload(context.TODO(), requestProgress, &azblob.UploadBlockBlobOptions{
		HTTPHeaders: &azblob.BlobHTTPHeaders{
			BlobContentType:        to.StringPtr("text/html; charset=utf-8"),
			BlobContentDisposition: to.StringPtr("attachment"),
		},
	})
	if err != nil {
		log.Print("Error while upload: ", err)
		//response.TaskStatus = "Failed"
	}

	wg.Done()
}

func processMapTask(request *mapreducepb.TaskRequest) string {
	shards := request.FileShard
	// Set map ID
	mapTaskID := request.MapTaskId

	var intermediateFilePath = fmt.Sprintf("./intermediate/map%02s", strconv.Itoa(int(mapTaskID)))

	var wg sync.WaitGroup

	log.Print("Map task")
	log.Printf("%+v", request.FileShard)

	for index, shard := range shards {
		if shard != nil {
			log.Println("Received shard: ", *shard)
			wg.Add(1)
			go processShard(index, int(request.MapTaskId), *shard, &wg)
		}
	}

	wg.Wait()

	log.Print("Map processed successfuly. Clearing files")
	err := os.RemoveAll("./intermediate")
	if err != nil {
		fmt.Println("Failed to clear intermediate map results on disk ", err)
	}

	return intermediateFilePath
}

// A Simple GRPC rpc ping call to check worker status
func (s *Server) SayHello(ctx context.Context, request *mapreducepb.PingMessage) (*mapreducepb.PingMessage, error) {
	log.Printf("Received message %s", request.Greeting)
	log.Printf("Replying message %s", (request.Greeting + " Alive!!"))

	// Context cancelled
	if ctx.Err() == context.Canceled {
		log.Println("Client done waiting")
		return nil, ctx.Err()
	}

	return &mapreducepb.PingMessage{Greeting: request.Greeting + " Alive!!"}, nil
}

// A grpc rpc to process map and reduce tasks.
func (s *Server) AssignTask(ctx context.Context, request *mapreducepb.TaskRequest) (*mapreducepb.TaskResponse, error) {
	log.Printf("Received task message for: %s", request.Type)

	log.Printf("Current Task counter is: %d", taskCounter)
	log.Printf("Failed Parameter is: %d", failedParameter)

	if failedParameter != 0 {
		if taskCounter == failedParameter {
			os.Exit(1)
		}
	}

	var response mapreducepb.TaskResponse
	if request.Type == "map" {
		path := processMapTask(request)

		response = mapreducepb.TaskResponse{
			TempFiles:  path,
			TaskStatus: config.Complete,
		}
	}

	if request.Type == "reduce" {
		paths := request.IntermediateFilePath
		var outputMap = make(map[string]float64)

		for _, path := range paths {
			log.Printf("Path is %+v", path)

			er := os.MkdirAll(path, os.ModePerm)
			if er != nil {
				log.Println("Failed to create intermediate directory.Error: ", er)
			}

			pathInAzure := path[15:]
			log.Println("Container path: ", pathInAzure)

			containerURLReduceInput := fmt.Sprintf("https://%s.blob.core.windows.net/intermediate", config.AccountName)
			containerClient := client.GetContainerClient(containerURLReduceInput)

			pager := containerClient.ListBlobsFlat(nil)
			var wg sync.WaitGroup

			for pager.NextPage(context.TODO()) {
				resp := pager.PageResponse()
				for _, blob := range resp.ContainerListBlobFlatSegmentResult.Segment.BlobItems {
					name := *blob.Name
					if strings.Contains(name, pathInAzure) {
						log.Println("BLOB NAME : ", name)
						wg.Add(1)

						go func(waitGroup *sync.WaitGroup) {
							blobClient := containerClient.NewBlockBlobClient(name)
							get, err := blobClient.Download(context.TODO(), &azblob.DownloadBlobOptions{})
							if err != nil {
								log.Println("Error while downloading")

							}

							// Wrap the response body in a ResponseBodyProgress and pass a callback function for progress reporting.
							responseBody := streaming.NewResponseProgress(
								get.Body(nil),
								func(bytesTransferred int64) {
									//fmt.Printf("Read %d of %d bytes.", bytesTransferred, *get.ContentLength)
								},
							)

							downloadedData := &bytes.Buffer{}
							_, err = downloadedData.ReadFrom(responseBody)
							if err != nil {
								log.Print(err)
							}

							err = responseBody.Close()
							if err != nil {
								log.Print(err)

							}

							writeString := downloadedData.String()

							intermediateFile := fmt.Sprintf("%s/map_%s.txt", path, randomString())
							f, err := os.Create(intermediateFile)
							if err != nil {
								log.Fatalf("Unable to create a file. Failed with error: %s", err.Error())
							}

							_, err = f.WriteString(writeString)
							if err != nil {
								log.Fatal("Unable to write to the file")
							}
							//fmt.Printf("wrote %d bytes\n", numberOfBytesWritten)
							f.Close()

							wg.Done()
						}(&wg)

						wg.Wait()
					}
				}
			}

			var output []byte
			var err error
			log.Println("Task Path is: ", path)
			c := exec.Command("python3", "./scripts/reducer.py", path)
			if output, err = c.Output(); err != nil {
				fmt.Println("Error: ", err)
				response.TaskStatus = "Failed"
			}

			/* To do code */
			var raw map[string]interface{}
			if err := json.Unmarshal(output, &raw); err != nil {
				fmt.Println(err)
			}

			for index, value := range raw {
				outputMap[index] += value.(float64)
			}
		}

		//var outputString string = fmt.Sprint(outputMap)
		var outputString string = createKeyValuePairs(outputMap)
		response = mapreducepb.TaskResponse{
			TaskStatus: config.Complete,
		}

		containerURLReduce := fmt.Sprintf("https://%s.blob.core.windows.net/final", config.AccountName)

		// Create an serviceClient object that wraps the service URL and a request pipeline to making requests.
		containerClientReduce := client.GetContainerClient(containerURLReduce)

		// Change this.
		blobClient := containerClientReduce.NewBlockBlobClient(fmt.Sprintf("reducer_%s", randomString()))

		// requestBody is the stream of data to write
		requestBody := streaming.NopCloser(strings.NewReader(string(outputString)))

		// Wrap the request body in a RequestBodyProgress and pass a callback function for progress reporting.
		requestProgress := streaming.NewRequestProgress(streaming.NopCloser(requestBody), func(bytesTransferred int64) {
			fmt.Printf("Wrote %d of %d bytes.", bytesTransferred, requestBody)
		})
		_, err := blobClient.Upload(context.TODO(), requestProgress, &azblob.UploadBlockBlobOptions{
			HTTPHeaders: &azblob.BlobHTTPHeaders{
				BlobContentType:        to.StringPtr("text/html; charset=utf-8"),
				BlobContentDisposition: to.StringPtr("attachment"),
			},
		})
		if err != nil {
			log.Print("Error while upload: ", err)
			response.TaskStatus = "Failed"
		} /*else {
			log.Print("Reduce processed successfuly. Clearing files")

			err = os.RemoveAll(request.IntermediateFilePath)
			if err != nil {
				fmt.Println("Failed to delete directory.Error: ", err)
			}
		}*/

	}

	taskCounter = taskCounter + 1

	return &response, nil
}

func main() {
	// create a listener on TCP port 8080
	if len(os.Args) < 3 {
		log.Fatalf("Port must be passed to start worker")
	}

	var err error
	port, err = strconv.Atoi(os.Args[1])
	if err != nil {
		log.Fatalf("Port not valid. Please part a valid and numeric port")
	}

	failParam, err := strconv.Atoi(os.Args[2])
	if err == nil {
		failedParameter = failParam
	}
	log.Print("Failed Parameter is: ", failedParameter)

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	signalChan := make(chan os.Signal, 100)
	signal.Notify(signalChan, syscall.SIGABRT, syscall.SIGKILL,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT,
	)

	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{"localhost:2379"}})
	if err != nil {
		log.Fatal(err)
	}
	defer cli.Close()

	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	kv := clientv3.NewKV(cli)
	key = "Worker_" + randomString()
	kv.Put(ctx, key, os.Getenv("WORKER_IP"))

	go func(kv clientv3.KV) {
		for true {
			select {
			case <-signalChan: // first signal, cancel context
				log.Print("Exit signal received. Deleting value")
				ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
				kv.Delete(ctx, key)

				cli.Close()
				os.Exit(1)
			}
		}
	}(kv)

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
