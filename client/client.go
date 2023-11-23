package client

import (
	"log"
	"os"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/workshop7/config"
)

// GetContainerClient returns a client
func GetContainerClient(containerURL string) azblob.ContainerClient {
	// Authenticate the client for accessing blob storage.
	accountName, accountKey := os.Getenv("AZURE_STORAGE_ACCOUNT_NAME"), os.Getenv("AZURE_STORAGE_ACCOUNT_KEY")
	accountName = config.AccountName
	accountKey = config.AccountKey
	credential, err := azblob.NewSharedKeyCredential(accountName, accountKey)
	if err != nil {
		log.Fatal(err)
	}

	containerClient, err := azblob.NewContainerClientWithSharedKey(containerURL, credential, nil)
	if err != nil {
		log.Fatal(err)
	}

	return containerClient
}
