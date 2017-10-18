package clouduploader

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/multi-cloud-storage-app/filelisting"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"bytes"
	"sort"
)

//CloudStorageProperties Common attributes for Cloud Storage
type CloudStorageProperties struct {
	SourceFiles   string
	TargetStorage string
	PartSize      int64
	Concurrency   int
	AccountKey    string
	SecretKey     string
	Region        string
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

type putObjectArgs struct {
	fileHandle 		  *os.File
	service    		  *s3.S3
	multiPartMetaData *s3.CreateMultipartUploadOutput
	putChannel        chan putPartResult
	seekPos    		  int64
	putSize    		  int64
	partNum           int64
}

type putPartResult struct {
	partId    string
	partNum   int64
}

//bleck, globals
var trace = false
var putObjectQueue chan putObjectArgs
var maxConns, activeConns int32
var putQueueSignal *sync.Cond
var condLock sync.Mutex

//UploadContent upload files and folders to Cloud Storage
func UploadContent(props CloudStorageProperties) (chan UploadResults, error) {
	initializePackageVariables(props)
	s3Service := getS3Service(props)

	go queuePutObjectRequests(putObjectQueue)
	uploadProgress := make(chan UploadResults)
	fileIterator := filelisting.ListFiles(props.SourceFiles)
	go processFile(uploadProgress, fileIterator, props, s3Service)
	return uploadProgress, nil
}

func initializePackageVariables(props CloudStorageProperties) {
	trace = props.Trace
	putObjectQueue = make(chan putObjectArgs, props.Concurrency)
	putQueueSignal = sync.NewCond(&condLock)
	maxConns = int32(props.Concurrency)
}

func getS3Service(props CloudStorageProperties) (* s3.S3) {
	var disableContinue bool = true
	var disableCheckSum bool = true
	config := aws.Config{
		MaxRetries: aws.Int(3),
		Region: &props.Region,
		S3Disable100Continue: &disableContinue,
		DisableComputeChecksums: &disableCheckSum,
	}
	if len(props.AccountKey) > 0 {
		creds := credentials.NewStaticCredentials(props.AccountKey, props.SecretKey, "")
		creds.Get()
		config.Credentials = creds
	}
	sess := session.Must( session.NewSession(&config))
	return s3.New(sess)
}

func queuePutObjectRequests(putObjectQueue chan putObjectArgs) {
	for request := range putObjectQueue {
		requestQueueSlot()
		go putPartUpload(request)
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

func processFile(uploadProgress chan UploadResults, fileProcess chan filelisting.FileHandle, props CloudStorageProperties, s3Service *s3.S3) {
	var completedPutObjects, fileThreads int64
	putObjectChannel := make(chan int)
	for fileInfo := range fileProcess {
		if !fileInfo.FileInfo.IsDir() {
			fileThreads++
			go uploadDataToCloud(s3Service, fileInfo, props, uploadProgress, putObjectChannel)
		}
	}
	for mon := range putObjectChannel {
		mon++
		completedPutObjects++
		if completedPutObjects >= fileThreads {
			close(putObjectChannel)
		}
	}
	close(uploadProgress)
}

func openSourceFile(source string) (*os.File, error) {
	fileHandle, err := os.Open(source)
	return fileHandle, err
}

func uploadDataToCloud(s3Service *s3.S3, fileInfo filelisting.FileHandle, props CloudStorageProperties, results chan UploadResults, putObjectChannel chan int) {
	objectName := extractObjectName(props.SourceFiles, fileInfo.FilePath)
	fileSize := fileInfo.FileInfo.Size()
	if trace {
		fmt.Printf("Uploading file %s, size %v bytes with part size %v MB\n", objectName, fileSize, props.PartSize)
	}
	startTime := time.Now().UnixNano()
	fileHandle, err := openSourceFile(fileInfo.FilePath)
	if err != nil {
		reportObjectResults(putObjectChannel, results, fileInfo.FileInfo, 0, err)
		return
	}

	multiPartUploadOutPut, err := createMultiPartUpload(s3Service, props.TargetStorage, objectName)
	if err != nil {
		reportObjectResults(putObjectChannel, results, fileInfo.FileInfo, 0, err)
		return
	}

	partSize := props.PartSize * 1024 * 1024
	parts := getNumberOfParts(partSize, fileSize)
	if trace {
		fmt.Printf("Uploading file %s, number of parts %v\n", objectName, parts)
	}
	putChannel := putParts(fileHandle, multiPartUploadOutPut, s3Service, partSize, fileSize, parts)
	putIDList, err := getPutResults(putChannel, parts)
	fileHandle.Close()
	if err != nil {
		reportObjectResults(putObjectChannel, results, fileInfo.FileInfo, 0, err)
		return
	}
	_, err = completeUpload(s3Service, multiPartUploadOutPut, putIDList)
	if err != nil {
		fmt.Printf("PUT CompleteUpload Failed %s, parts %v, list len %v\n", fileInfo.FileInfo.Name(), parts, len(putIDList))
	}

	endTime := time.Now().UnixNano()
	reportObjectResults(putObjectChannel, results, fileInfo.FileInfo, (endTime - startTime), err)
}

func reportObjectResults(putObjectChannel chan int, results chan UploadResults, fileInfo os.FileInfo, duration int64, err error) {
	var success = true
	if err != nil {
		success = false
	}
	results <- UploadResults{FileName: fileInfo.Name(), Bytes: fileInfo.Size(), Status: success, Err: err, Duration: duration}
	putObjectChannel <- 1
}


func createMultiPartUpload( s3Service * s3.S3, bucket, objectKey string ) (*s3.CreateMultipartUploadOutput, error) {
	input := s3.CreateMultipartUploadInput{
		Bucket: aws.String(bucket),
		Key: aws.String(objectKey),
	}
	output, err := s3Service.CreateMultipartUpload(&input)
	return output, err
}

func completeUpload(s3Service *s3.S3, multiPartUploadOutPut *s3.CreateMultipartUploadOutput, putIDList []putPartResult) (* s3.CompleteMultipartUploadOutput, error) {
	completeParts := make([]*s3.CompletedPart, len(putIDList))
	for i := 0; i < len(putIDList); i++ {
		completeParts[i] = new(s3.CompletedPart)
		completeParts[i].ETag = aws.String(putIDList[i].partId)
		completeParts[i].PartNumber = aws.Int64(putIDList[i].partNum)
	}
	sort.Slice(completeParts[:],func(i, j int) bool {
		return *completeParts[i].PartNumber < *completeParts[j].PartNumber
	})

	input := s3.CompleteMultipartUploadInput{
		Bucket: multiPartUploadOutPut.Bucket,
		Key:    multiPartUploadOutPut.Key,
		UploadId: multiPartUploadOutPut.UploadId,
		MultipartUpload: &s3.CompletedMultipartUpload{
			Parts: completeParts,
		},
	}
	return s3Service.CompleteMultipartUpload(&input)
}

func getPutResults(putChannel chan putPartResult, parts int64) ([]putPartResult, error) {
	putResultList := make([]putPartResult, parts)

	var completedParts, errorParts int64
	for block := range putChannel {
		if len(block.partId) == 0 {
			errorParts++
		} else {
			putResultList[completedParts] = block
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
	return putResultList, err
}

func putParts(fileHandle *os.File, multiPartUploadOuput * s3.CreateMultipartUploadOutput, s3Service *s3.S3, partSize,fileSize, parts int64) chan putPartResult {
	putPartChannel := make(chan putPartResult)
	var putSize int64
	for i := int64(0); i < parts; i++ {
		seekPos := i * partSize
		if (seekPos + partSize) <= fileSize {
			putSize = partSize
		} else {
			putSize = (fileSize - seekPos)
		}
		putObjectQueue <- putObjectArgs{fileHandle: fileHandle, service: s3Service, multiPartMetaData: multiPartUploadOuput, putChannel: putPartChannel, seekPos: seekPos, putSize: putSize, partNum: (i+1)}
	}
	return putPartChannel
}

func extractObjectName(prefix, filePath string) string {
	var fileName string
	if strings.Compare(strings.ToLower(prefix), strings.ToLower(filePath)) == 0 {
		_, fileName = filepath.Split(filePath)
	} else {
		var err error
		fileName, err = filepath.Rel(prefix, filePath)
		if err != nil {
			fileName = filePath
		}
	}
	return fileName
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

func putPartUpload( partUploadData putObjectArgs ) {
	var partOutPut *s3.UploadPartOutput = nil
	var etag string = ""
	readArray, err := readFileBytes(partUploadData.fileHandle, partUploadData.seekPos, partUploadData.putSize)
	if err == nil {
		partOutPut, err = putPart(partUploadData, readArray)
	}
	if err != nil {
		fmt.Printf("Failure to PUT block, %s\n", err)
	} else if trace {
		fmt.Printf("Uploaded Part %s, Size %v\n", *partOutPut.ETag, partUploadData.putSize)
	}
	releaseQueueSlot()
	if partOutPut != nil {
		etag = *partOutPut.ETag;
	}
	partUploadData.putChannel <- putPartResult{partId: etag, partNum: partUploadData.partNum}
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

func putPart(partUploadData putObjectArgs, data []byte ) (*s3.UploadPartOutput, error){
	fileBytes := bytes.NewReader(data)
	input := s3.UploadPartInput{
		Bucket:     partUploadData.multiPartMetaData.Bucket,
		Key:        partUploadData.multiPartMetaData.Key,
		PartNumber: &partUploadData.partNum,
		UploadId:   partUploadData.multiPartMetaData.UploadId,
		Body:		fileBytes,
	}
	return partUploadData.service.UploadPart(&input)
}