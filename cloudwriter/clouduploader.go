package clouduploader

import (
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/Azure/azure-sdk-for-go/storage"
	"github.com/multi-cloud-storage-app/filelisting"
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
	FileName string
	Bytes    int64
	Duration int64
	Status   bool
	Err      error
}

//UploadContent upload files and folders to Cloud Storage
func UploadContent(props CloudStorageProperties) (chan UploadResults, error) {
	blobClient, err := getBlobCLI(props.Account, props.Key)
	if err != nil {
		return nil, err
	}
	container, err := setUpTargetFolder(props.TargetStorage, blobClient)
	if err != nil {
		return nil, err
	}

	uploadProgress := make(chan UploadResults)
	fileIterator := filelisting.ListFiles(props.SourceFiles)
	go processFile(uploadProgress, fileIterator, props, container)
	return uploadProgress, nil
}

func processFile(uploadProgress chan UploadResults, fileProcess chan filelisting.FileHandle, props CloudStorageProperties, container *storage.Container) {
	var completedPutBlobs, fileThreads int64
	putBlobChannel := make(chan int)
	for fileInfo := range fileProcess {
		if !fileInfo.FileInfo.IsDir() {
			fileThreads++
			go uploadDataToBlob(container, fileInfo, props, uploadProgress, putBlobChannel)
		}
	}
	for mon := range putBlobChannel {
		mon++
		completedPutBlobs++
		if completedPutBlobs >= fileThreads {
			close(putBlobChannel)
		}
	}
	close(uploadProgress)
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

func uploadDataToBlob(container *storage.Container, fileInfo filelisting.FileHandle, props CloudStorageProperties, results chan UploadResults, putBlobChannel chan int) {
	blobName := extractBlobName(fileInfo.FilePath, fileInfo.FileInfo.Name())
	fileSize := fileInfo.FileInfo.Size()
	fmt.Printf("Uploading file %s, size %v bytes with part size %v MB\n", blobName, fileSize, props.PartSize)

	startTime := time.Now().UnixNano()
	blob := container.GetBlobReference(blobName)
	err := blob.CreateBlockBlob(nil)
	if err != nil {
		results <- UploadResults{FileName: fileInfo.FileInfo.Name(), Bytes: fileSize, Status: false, Err: err}
		putBlobChannel <- 1
		return
	}
	fileHandle, err := openSourceFile(props.SourceFiles)
	if err != nil {
		results <- UploadResults{FileName: fileInfo.FileInfo.Name(), Bytes: fileSize, Status: false, Err: err}
		putBlobChannel <- 1
		return
	}
	defer fileHandle.Close()

	partSize := props.PartSize * 1024 * 1024
	parts := getNumberOfParts(partSize, fileSize)
	fmt.Printf("Uploading file %s, number of parts %v\n", blobName, parts)
	putChannel := putParts(fileHandle, blob, partSize, fileSize, parts)

	blockIDList, err := getPutResults(putChannel, parts)
	if err != nil {
		results <- UploadResults{FileName: fileInfo.FileInfo.Name(), Bytes: fileSize, Status: false, Err: err}
		putBlobChannel <- 1
		return
	}
	err = blob.PutBlockList(blockIDList, nil)
	var success = true
	if err != nil {
		success = false
	}
	endTime := time.Now().UnixNano()
	results <- UploadResults{FileName: fileInfo.FileInfo.Name(), Bytes: fileSize, Status: success, Err: err, Duration: (endTime - startTime)}
	putBlobChannel <- 1
}

func getPutResults(putChannel chan storage.Block, parts int64) ([]storage.Block, error) {
	blockIDList := make([]storage.Block, parts)

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
	var err error
	if errorParts > 0 {
		err = errors.New(fmt.Sprintf("Upload parts in error %v", errorParts))
	}
	return blockIDList, err
}

func putParts(fileHandle *os.File, blob *storage.Blob, partSize, fileSize, parts int64) chan storage.Block {
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
	return putChannel
}

func extractBlobName(prefix, filePath string) string {
	fileName, err := filepath.Rel(prefix, filePath)
	if err != nil {
		fileName = filePath
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
