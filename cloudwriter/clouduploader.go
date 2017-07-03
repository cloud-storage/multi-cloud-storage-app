package clouduploader

import (
	"encoding/base64"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/storage"
	"github.com/nu7hatch/gouuid"
)

//CloudStorageProperties Common attributes for Cloud Storage
type CloudStorageProperties struct {
	SourceFiles   string
	TargetStorage string
	PartSize      int64
	Concurrency   int
	Account       string
	Key           string
}

//UploadResults data for successful upload
type UploadResults struct {
	Bytes    uint64
	Files    uint64
	Duration uint64
}

//UploadContent upload files and folders to Cloud Storage
func UploadContent(props CloudStorageProperties) (UploadResults, error) {
	startTime := time.Now().UnixNano()
	results := UploadResults{0, 0, 0}
	fileHandle, err := openSourceFile(props.SourceFiles)
	if err != nil {
		return results, err
	}
	defer fileHandle.Close()
	blobClient, err := getBlobCLI(props.Account, props.Key)
	if err != nil {
		return results, err
	}
	cnt, err := setUpTargetFolder(props.TargetStorage, blobClient)
	if err != nil {
		return results, err
	}
	blobName := extractBlobName(props.SourceFiles)
	bytes, err := uploadDataToBlob(blobName, cnt, fileHandle, props)
	if err == nil {
		endTime := time.Now().UnixNano()
		results.Bytes = bytes
		results.Files = 1
		results.Duration = uint64((endTime - startTime) / 1000000)
	}
	return results, nil
}

func openSourceFile(source string) (*os.File, error) {
	fileHandle, err := os.Open(source)
	return fileHandle, err
}

func getBlobCLI(account string, key string) (storage.BlobStorageClient, error) {
	var blobClient storage.BlobStorageClient
	client, err := storage.NewBasicClient(account, key)
	if err != nil {
		return blobClient, err
	}
	blobClient = client.GetBlobService()
	return blobClient, nil
}

func setUpTargetFolder(targetStorage string, blobClient storage.BlobStorageClient) (*storage.Container, error) {
	cnt := blobClient.GetContainerReference(targetStorage)
	options := storage.CreateContainerOptions{
		Access: storage.ContainerAccessTypePrivate,
	}
	_, err := cnt.CreateIfNotExists(&options)
	return cnt, err
}

func uploadDataToBlob(blobName string, container *storage.Container, fileHandle *os.File, props CloudStorageProperties) (uint64, error) {
	var bytes uint64
	blob := container.GetBlobReference(blobName)
	err := blob.CreateBlockBlob(nil)
	if err != nil {
		return 0, err
	}
	fileInfo, err := fileHandle.Stat()
	if err != nil {
		return bytes, err
	}
	fileSize := fileInfo.Size()
	fmt.Printf("Uploading file %s, size %v bytes with part size %v MB\n", blobName, fileSize, props.PartSize)
	partSize := props.PartSize * 1024 * 1024

	parts := getNumberOfParts(partSize, fileSize)

	blockIDList := make([]storage.Block, parts)
	fmt.Printf("Uploading file %s, number of parts %v\n", blobName, parts)

	putChannel := make(chan storage.Block)
	var putSize int64
	for i := int64(0); i < parts; i++ {
		seekPos := i * partSize
		if (seekPos + partSize) <= fileSize {
			putSize = partSize
		} else {
			putSize = (fileSize - seekPos)
		}
		go putBlock(fileHandle, blob, putChannel, seekPos, putSize)
	}

	var completedParts, errorParts int64
	for block := range putChannel {
		if len(block.ID) == 0 {
			errorParts++
		} else {
			blockIDList[completedParts] = block
		}
		completedParts++
		if completedParts >= parts {
			close(putChannel)
		}
	}

	fmt.Printf("Uploaded parts, success %v, failed %v\n", completedParts, errorParts)

	if errorParts > 0 {
		fmt.Println("Failure uploading parts")
	} else {
		err = blob.PutBlockList(blockIDList, nil)
		if err != nil {
			return 0, err
		}
	}

	return uint64(fileSize), nil
}

func extractBlobName(filePath string) string {
	var fileName string
	index := strings.LastIndex(filePath, "/")
	if index == -1 {
		fileName = filePath
	} else {
		fileName = filePath[(index + 1):len(filePath)]
	}
	return fileName
}

func getBlockID() string {
	uuid, _ := uuid.NewV4()
	return base64.StdEncoding.EncodeToString([]byte(uuid.String()))
}

func getNumberOfParts(partSize, fileSize int64) int64 {
	var parts int64
	if partSize >= fileSize {
		parts = 1
	} else {
		parts = fileSize / partSize
		if fileSize > (parts * partSize) {
			parts++
		}
	}
	return parts
}

func putBlock(fileHandle *os.File, blob *storage.Blob, putChannel chan storage.Block, offSet int64, putSize int64) {
	readArray := make([]byte, putSize)
	blockID := getBlockID()
	fileHandle.ReadAt(readArray, offSet)
	err := blob.PutBlock(blockID, readArray, nil)
	if err == nil {
		fmt.Printf("Uploaded Part %s, Size %v\n", blockID, putSize)
	} else {
		blockID = ""
		fmt.Printf("Failure to PUT block, %s\n", err)
	}
	putChannel <- storage.Block{ID: blockID, Status: storage.BlockStatusUncommitted}
}
