package clouduploader

import (
	"encoding/base64"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
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
	Trace         bool
}

//UploadResults data for successful upload
type UploadResults struct {
	FileName string
	Bytes    int64
	Duration int64
	Status   bool
	Err      error
}

type putBlobArgs struct {
	fileHandle *os.File
	blob       *storage.Blob
	putChannel chan storage.Block
	seekPos    int64
	putSize    int64
}

//bleck, globals
var trace = false
var putBlobQueue chan putBlobArgs
var maxConns, activeConns int32
var putQueueSignal *sync.Cond
var condLock sync.Mutex

//UploadContent upload files and folders to Cloud Storage
func UploadContent(props CloudStorageProperties) (chan UploadResults, error) {
	initiailizePackageVariables(props.Concurrency, props.Trace)
	container, err := getContainer(props.Account, props.Key, props.TargetStorage)
	if err != nil {
		return nil, err
	}

	go queuePutBlobRequests(putBlobQueue)
	uploadProgress := make(chan UploadResults)
	fileIterator := filelisting.ListFiles(props.SourceFiles)
	go processFile(uploadProgress, fileIterator, props, container)
	return uploadProgress, nil
}

func initiailizePackageVariables(concurrency int, traceOut bool) {
	trace = traceOut
	putBlobQueue = make(chan putBlobArgs, concurrency)
	putQueueSignal = sync.NewCond(&condLock)
	maxConns = int32(concurrency)
}

func getContainer(account, key, containerName string) (*storage.Container, error) {
	var container *storage.Container
	blobClient, err := getBlobCLI(account, key)
	if err == nil {
		container, err = setUpTargetFolder(containerName, blobClient)
	}
	return container, err
}

func queuePutBlobRequests(putBlobQueue chan putBlobArgs) {
	for request := range putBlobQueue {
		requestQueueSlot()
		go putBlock(request.fileHandle, request.blob, request.putChannel, request.seekPos, request.putSize)
	}
}

func requestQueueSlot() {
	currActive := atomic.LoadInt32(&activeConns)
	putQueueSignal.L.Lock()
	for currActive >= maxConns {
		putQueueSignal.Wait()
		currActive = atomic.LoadInt32(&activeConns)
	}
	putQueueSignal.L.Unlock()
	atomic.AddInt32(&activeConns, 1)
}

func releaseQueueSlot() {
	atomic.AddInt32(&activeConns, -1)
	putQueueSignal.Signal()
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
	blobName := extractBlobName(props.SourceFiles, fileInfo.FilePath)
	fileSize := fileInfo.FileInfo.Size()
	if trace {
		fmt.Printf("Uploading file %s, size %v bytes with part size %v MB\n", blobName, fileSize, props.PartSize)
	}
	startTime := time.Now().UnixNano()
	blob, err := getBlob(container, blobName)
	if err != nil {
		reportBlobResults(putBlobChannel, results, fileInfo.FileInfo, 0, err)
		//		results <- UploadResults{FileName: fileInfo.FileInfo.Name(), Bytes: fileSize, Status: false, Err: err}
		//		putBlobChannel <- 1
		return
	}
	fileHandle, err := openSourceFile(fileInfo.FilePath)
	if err != nil {
		reportBlobResults(putBlobChannel, results, fileInfo.FileInfo, 0, err)
		//		results <- UploadResults{FileName: fileInfo.FileInfo.Name(), Bytes: fileSize, Status: false, Err: err}
		//		putBlobChannel <- 1
		return
	}

	partSize := props.PartSize * 1024 * 1024
	parts := getNumberOfParts(partSize, fileSize)
	if trace {
		fmt.Printf("Uploading file %s, number of parts %v\n", blobName, parts)
	}
	putChannel := putParts(fileHandle, blob, partSize, fileSize, parts)
	blockIDList, err := getPutResults(putChannel, parts)
	fileHandle.Close()
	if err != nil {
		reportBlobResults(putBlobChannel, results, fileInfo.FileInfo, 0, err)
		//		results <- UploadResults{FileName: fileInfo.FileInfo.Name(), Bytes: fileSize, Status: false, Err: err}
		//		putBlobChannel <- 1
		return
	}
	err = putBlobBlockList(blob, blockIDList)
	if err != nil {
		fmt.Printf("PUT BlockList Failed %s, parts %v, list len %v\n", fileInfo.FileInfo.Name(), parts, len(blockIDList))
		fmt.Printf("PUT BlockList Failed %s, block list %v\n", fileInfo.FileInfo.Name(), blockIDList)
	}

	endTime := time.Now().UnixNano()
	reportBlobResults(putBlobChannel, results, fileInfo.FileInfo, (endTime - startTime), err)
	//	results <- UploadResults{FileName: fileInfo.FileInfo.Name(), Bytes: fileSize, Status: success, Err: err, Duration: (endTime - startTime)}
	//	putBlobChannel <- 1
}

func reportBlobResults(putBlobChannel chan int, results chan UploadResults, fileInfo os.FileInfo, duration int64, err error) {
	var success = true
	if err != nil {
		success = false
	}
	results <- UploadResults{FileName: fileInfo.Name(), Bytes: fileInfo.Size(), Status: success, Err: err, Duration: duration}
	putBlobChannel <- 1
}

func getBlob(container *storage.Container, blobName string) (*storage.Blob, error) {
	blob := container.GetBlobReference(blobName)
	err := blob.CreateBlockBlob(nil)
	return blob, err
}

func putBlobBlockList(blob *storage.Blob, list []storage.Block) error {
	err := blob.PutBlockList(list, nil)
	return err
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
		err = fmt.Errorf("Upload parts in error %v", errorParts)
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
		putBlobQueue <- putBlobArgs{fileHandle: fileHandle, blob: blob, putChannel: putChannel, seekPos: seekPos, putSize: putSize}
	}
	return putChannel
}

func extractBlobName(prefix, filePath string) string {
	var fileName string
	if strings.Compare(prefix, filePath) == 0 {
		fileName = path.Base(filePath)
	} else {
		var err error
		fileName, err = filepath.Rel(prefix, filePath)
		if err != nil {
			fileName = filePath
		}
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

func putBlock(fileHandle *os.File, blob *storage.Blob, putChannel chan storage.Block, offSet, putSize int64) {
	blockID := getBlockID()
	readArray, err := readFileBytes(fileHandle, offSet, putSize)
	if err == nil {
		err = blob.PutBlock(blockID, readArray, nil)
	}
	if err != nil {
		blockID = ""
		fmt.Printf("Failure to PUT block, %s\n", err)
	} else if trace {
		fmt.Printf("Uploaded Part %s, Size %v\n", blockID, putSize)
	}
	releaseQueueSlot()
	putChannel <- storage.Block{ID: blockID, Status: storage.BlockStatusUncommitted}
}

func readFileBytes(file *os.File, offSet, putSize int64) ([]byte, error) {
	readArray := make([]byte, putSize)
	bytes, err := file.ReadAt(readArray, offSet)
	if err != nil {
		fmt.Printf("Failure to Read File on PUT %s, %s\n", file.Name(), err)
	}
	if int64(bytes) != putSize {
		err = fmt.Errorf("Failure to Read File on PUT Size incorrect, bytes %v, read size %v\n", bytes, putSize)
	}
	return readArray, err
}
