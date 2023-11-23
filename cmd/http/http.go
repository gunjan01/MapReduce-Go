package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/gorilla/mux"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type Job struct {
	ID   string `json:"int"`
	Type string `json:"type"`
}

// randomString generates a randome string
func randomString() string {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	return strconv.Itoa(r.Int())
}

func main() {
	routes := mux.NewRouter()
	routes.HandleFunc("/job", func(w http.ResponseWriter, r *http.Request) {
		decoder := json.NewDecoder(r.Body)

		log.Print("Received http request for job submission")

		var job Job
		err := decoder.Decode(&job)
		if err != nil {
			log.Fatalf("Unable to decode the request")
		}

		log.Printf("Successfully decoded job of type: %s", job.Type)

		job.ID = randomString()

		// Create a etcd client
		log.Print("Connecting to etcd")
		cli, err := clientv3.New(clientv3.Config{
			Endpoints:   []string{"localhost:2379"},
			DialTimeout: 10 * time.Second,
		})
		if err != nil {
			log.Fatal("Could not establish connection. Failed with err: ", err)
			os.Exit(1)
		}
		defer cli.Close()

		/* Getting the worker pod ips for load balancing" */
		kv := clientv3.NewKV(cli)

		gr, _ := kv.Get(context.Background(), "Leader")
		fmt.Println("Value: ", string(gr.Kvs[0].Value), "Revision: ", gr.Header.Revision)

		ip := string(gr.Kvs[0].Value)

		log.Printf("Key is %s", ip)

		// Forward request to elected master
		url := fmt.Sprintf("http://%s:80/job/submit", ip)

		payloadBuf := new(bytes.Buffer)
		json.NewEncoder(payloadBuf).Encode(&job)
		req, _ := http.NewRequest("POST", url, payloadBuf)

		client := &http.Client{}
		res, err := client.Do(req)
		if err != nil {
			log.Println("Error while sending the request")
		}

		defer res.Body.Close()

		fmt.Println("response Status:", res.Status)

	}).Methods("POST")

	log.Println("Starting http server on port 8080")
	err := http.ListenAndServe(":8080", routes)
	if err != nil {
		log.Fatalf("Unable to start the server at port 8080")
	}
}
