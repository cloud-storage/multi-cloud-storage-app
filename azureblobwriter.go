package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/multi-cloud-storage-app/cloudwriter"
)

func main() {
	fmt.Println("Azure Blob Uploader")

	sourceToUpload := flag.String("source", "", "File/Folder to Upload")
	container := flag.String("container", "", "Container Name")
	partSize := flag.Int64("part-size", 64, "Part Size in MB")
	concurrency := flag.Int("concurrency", 24, "Threads")
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

	fmt.Printf("Upload %s with %v MB part size and %v threads\n", *sourceToUpload, *partSize, *concurrency)
	//	fmt.Printf("Using account %s and key %s\n", accountName, accountKey)
	startTime := time.Now().UnixNano()
	uploadProperties := clouduploader.CloudStorageProperties{SourceFiles: *sourceToUpload, TargetStorage: *container, PartSize: *partSize, Concurrency: *concurrency, Account: accountName, Key: accountKey}
	results, err := clouduploader.UploadContent(uploadProperties)
	if err != nil {
		errorAndExit(err)
	}

	var fileCount, byteCount int64
	for fileResult := range results {
		fmt.Printf("Upload result: File %s, Bytes %v, Status %v, Duration %v\n", fileResult.FileName, fileResult.Bytes, fileResult.Status, fileResult.Duration)
		fileCount++
		byteCount += fileResult.Bytes
	}
	endTime := time.Now().UnixNano()
	duration := (endTime - startTime) / 1000000
	durationSec := float64(duration) / float64(1000)
	bitCount := float64(byteCount) * 8
	//	fmt.Printf("Duration MS is %v, Seconds %v, Bits %v\n", duration, durationSec, bitCount)
	rate := bitCount / durationSec
	fmt.Printf("Uploaded Files %v,  Rate %.6f bps, %v Bytes, Duration %v (ms)\n", fileCount, rate, byteCount, duration)

	fmt.Println("Azure Blob Uploader Complete")
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
	fmt.Printf("Uploader exiting; %s\n", err.Error())
	os.Exit(1)
}
