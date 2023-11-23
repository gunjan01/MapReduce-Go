package main

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"math"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/streaming"
	"github.com/workshop7/client"
	"github.com/workshop7/config"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
)

// Shard is a struct
type Shard struct {
	ShardNumber int
	Filename    string
	Start       int64
	End         int64
}

var fileArray = []string{}
var totalSize float64

// ShardFiles shards files and returns an array of shards.
func ShardFiles() map[int][]Shard {

	var (
		startOffset      int64 = 0
		shardNumber      int   = 0
		fileNumber       int   = 0
		currentShardSize int64 = 0
	)

	shards := make(map[int][]Shard)
	fileSizes := make(map[string]int64) //store file size here

	containerClient := client.GetContainerClient(config.ContainerURL)

	pager := containerClient.ListBlobsFlat(nil)

	for pager.NextPage(context.TODO()) {
		resp := pager.PageResponse()
		for _, blob := range resp.ContainerListBlobFlatSegmentResult.Segment.BlobItems {
			var size int64

			if blob.Properties != nil {
				size = *blob.Properties.ContentLength
			}

			fileArray = append(fileArray, *blob.Name)
			fileSizes[*blob.Name] = size
			fmt.Printf("Blob name: %s,  size: %d\n", *blob.Name, size)

			totalSize += float64(size)
		}
	}

	NumberOfShards := math.Ceil(totalSize / float64(config.ShardSize))
	//NumberOfShardsPerWorker := math.Ceil(NumberOfShards / float64(config.NumberOfWorkers))

	fmt.Printf("Total Size to be processed: %f \n", totalSize)
	fmt.Printf("No of shards are %d \n", int64(NumberOfShards))
	//fmt.Printf("Each worker processes %d shards", int64(NumberOfShardsPerWorker))

	for fileNumber < len(fileArray) && shardNumber < int(NumberOfShards) {
		fileSize := fileSizes[fileArray[fileNumber]]

		if (currentShardSize + fileSize) < config.ShardSize {
			shard := Shard{
				ShardNumber: shardNumber,
				Filename:    fileArray[fileNumber],
				Start:       startOffset,
				End:         fileSize,
			}

			if startOffset != 0 {
				shard.End = fileSize + startOffset
			}

			shards[shardNumber] = append(shards[shardNumber], shard)

			fileNumber += 1
			startOffset = 0
			currentShardSize += fileSize

		} else if (config.ShardSize - currentShardSize) > 0 {
			remainingSpace := (config.ShardSize - currentShardSize)
			log.Printf("Remaining space in the shard: %d", remainingSpace)

			blobClient := containerClient.NewBlockBlobClient(fileArray[fileNumber])
			offset := int64(startOffset + remainingSpace)
			count := int64(30)

			get, err := blobClient.Download(context.TODO(), &azblob.DownloadBlobOptions{Offset: &offset, Count: &count})

			// Wrap the response body in a ResponseBodyProgress and pass a callback function for progress reporting.
			responseBody := streaming.NewResponseProgress(
				get.Body(nil),
				func(bytesTransferred int64) {
					fmt.Printf("Read %d of %d bytes.", bytesTransferred, *get.ContentLength)
				},
			)

			downloadedData := &bytes.Buffer{}
			_, err = downloadedData.ReadFrom(responseBody)
			if err != nil {
				log.Fatal(err)
			}

			err = responseBody.Close()
			if err != nil {
				log.Fatal(err)
			}

			var numberOfCharacters int64
			var line string

			reader := bufio.NewReader(downloadedData)

			for {
				line, err = reader.ReadString(' ')
				if err != nil && err != io.EOF {
					log.Fatalf("Unable to read the file until whitespace is encountered")
					break
				}

				fmt.Println("Read characters: ", len(line))
				numberOfCharacters += int64(len(line))

				break
			}

			shard := Shard{
				ShardNumber: shardNumber,
				Filename:    fileArray[fileNumber],
				Start:       startOffset,
				End:         startOffset + remainingSpace + numberOfCharacters,
			}
			shards[shardNumber] = append(shards[shardNumber], shard)

			shardNumber += 1
			currentShardSize = 0
			startOffset = startOffset + remainingSpace + numberOfCharacters

			fileSizes[fileArray[fileNumber]] = fileSizes[fileArray[fileNumber]] - (remainingSpace + numberOfCharacters)

			fmt.Println("Resized file: ", fileSizes[fileArray[fileNumber]])
		} else {
			log.Fatalf("Unexpected error occured while sharding")
		}
	}

	return shards
}
