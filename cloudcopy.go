package main

import (
	"container/list"
	"errors"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/multi-cloud-storage-app/cloudwriter"
)

func main() {
	fmt.Println("Cloud Copy Data Uploader")

	sourceToUpload := flag.String("source", "", "File/Folder to Upload")
	container := flag.String("container", "", "Container Name")
	partSize := flag.Int64("part-size", 100, "Part Size in MB")
	concurrency := flag.Int("concurrency", 128, "Concurrent Parts")
	trace := flag.Bool("trace", false, "Trace Progress")
	stats := flag.Bool("stats", true, "Log File Stats")
	flag.Parse()

	err := validateInput(*sourceToUpload, *container)
	if err != nil {
		flag.Usage()
		errorAndExit(err)
	}

	accountName, accountKey, err := getAccountInfoEnv()
	if err != nil {
		errorAndExit(err)
	}

	fmt.Printf("Upload %s with %v MB Part Size and %v Concurrent Parts\n", *sourceToUpload, *partSize, *concurrency)
	startTime := time.Now().UnixNano()
	uploadProperties := clouduploader.CloudStorageProperties{SourceFiles: *sourceToUpload, TargetStorage: *container, PartSize: *partSize, Concurrency: *concurrency, Account: accountName, Key: accountKey, Trace: *trace}
	results, err := clouduploader.UploadContent(uploadProperties)
	if err != nil {
		errorAndExit(err)
	}

	uploadResults := list.New()
	var fileCount, byteCount int64
	for fileResult := range results {
		uploadResults.PushBack(fileResult)
		if *trace {
			fmt.Printf("Upload result: File %s, Bytes %v, Status %v, Duration %v\n", fileResult.FileName, fileResult.Bytes, fileResult.Status, fileResult.Duration)
		}
		fileCount++
		byteCount += fileResult.Bytes
	}
	endTime := time.Now().UnixNano()
	displayBasicStats((endTime - startTime), fileCount, byteCount)
	if *stats {
		displayFileStats(uploadResults)
	}

	fmt.Println("Cloud Copy Uploader Complete")
}

func displayFileStats(fileStats *list.List) {
	fmt.Println("File Statistics")
	var status string
	for result := fileStats.Front(); result != nil; result = result.Next() {
		fileData := result.Value.(clouduploader.UploadResults)
		if fileData.Status {
			status = "Success"
		} else {
			status = "Failed"
		}
		fmt.Printf("%s - File %s, Bytes %v, Duration %.3f\n", status, fileData.FileName, fileData.Bytes, (float64(fileData.Duration) / float64(1000000)))
	}
}

func displayBasicStats(duration, fileCount, byteCount int64) {
	durationMs := duration / 1000000
	durationSec := float64(durationMs) / float64(1000)
	bitCount := float64(byteCount) * 8
	rate := bitCount / durationSec
	fmt.Printf("Uploaded Files %v,  Rate %.6f bps, %v Bytes, Duration %v (ms)\n", fileCount, rate, byteCount, durationMs)
}

func validateInput(source string, container string) error {
	if len(source) == 0 {
		return errors.New("Source file or folder to upload must be specified")
	}
	if len(container) == 0 {
		return errors.New("Container must be specified")
	}
	_, err := os.Stat(source)
	if err != nil {
		return errors.New("Source file/folder not found, " + source)
	}
	return nil
}

func getAccountInfoEnv() (string, string, error) {
	accountName := os.Getenv("ACCOUNT_NAME")
	accountKey := os.Getenv("ACCOUNT_KEY")
	if len(accountName) == 0 || len(accountKey) == 0 {
		return "", "", errors.New("Environment variable ACCOUNT_NAME/ACCOUNT_KEY not set")
	}
	return accountName, accountKey, nil
}

func errorAndExit(err error) {
	fmt.Printf("Cloud Copy exiting; %s\n", err.Error())
	os.Exit(1)
}
