package main

import (
	"errors"
	"flag"
	"fmt"
	"os"

	"github.com/azureblobwriter/cloudwriter"
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

	fmt.Printf("Using account %s and key %s\n", accountName, accountKey)
	uploadProperties := clouduploader.CloudStorageProperties{SourceFiles: *sourceToUpload, TargetStorage: *container, PartSize: *partSize, Concurrency: *concurrency, Account: accountName, Key: accountKey}
	results, err := clouduploader.UploadContent(uploadProperties)

	if err == nil {
		fmt.Printf("Upload Successful, %v bytes, %v files, %v duration (ms)\n", results.Bytes, results.Files, results.Duration)
		rate := ((float64(results.Bytes) * 8) / (float64(results.Duration) / 1000))
		fmt.Printf("Upload rate %.6f bps, %v bytes, %v duration (ms)\n", rate, results.Bytes, results.Duration)
	} else {
		fmt.Printf("Uplaod Failed, %s\n", err)
	}
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
