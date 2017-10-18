package main

import (
	"container/list"
	"errors"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/multi-cloud-storage-app/s3writer"
	"github.com/multi-cloud-storage-app/filelisting"
	"strings"
)

func main() {
	fmt.Println("Cloud Copy Data Uploader")

	sourceToUpload := flag.String("source", "", "File/Folder to Upload")
	container := flag.String("container", "", "Container Name")
	partSize := flag.Int64("part-size", 100, "Part Size in MB")
	concurrency := flag.Int("concurrency", 128, "Concurrent Parts")
	trace := flag.Bool("trace", false, "Trace Operations")
	stats := flag.Bool("stats", false, "Display File Stats")
	progress := flag.Bool("progress", false, "Display Progress")
	platform := flag.String( "platform", "azure", "Cloud Storage < aws | azure >")
	flag.Parse()

	err := validateInput(*sourceToUpload, *container, *platform)
	if err != nil {
		flag.Usage()
		errorAndExit(err)
	}

	accountName, accountKey := getAccountInfoEnv()
	if (*platform == "azure"){
		err := validateAccountInfo(accountName, accountKey)
		if err != nil {
			errorAndExit(err)
		}
	}

	regionName := getRegionFromEnv()

	if (*platform == "aws") {
		err := validateRegion(regionName)
		if err != nil {
			errorAndExit(err)
		}
	}

	var totalBytes, totalFiles int64
	if *progress {
		totalBytes, totalFiles, err = filelisting.GetSizes(*sourceToUpload)
		if err != nil {
			errorAndExit(err)
		}
	}

	fmt.Printf("Upload to %s, %s with %v MB Part Size and %v Concurrent Parts\n", *platform, *sourceToUpload, *partSize, *concurrency)
	startTime := time.Now().UnixNano()
	uploadProperties := clouduploader.CloudStorageProperties{SourceFiles: *sourceToUpload, TargetStorage: *container, PartSize: *partSize, Concurrency: *concurrency, AccountKey: accountName, SecretKey: accountKey, Region: regionName, Trace: *trace}
	results, err := clouduploader.UploadContent(uploadProperties)
	if err != nil {
		errorAndExit(err)
	}

	uploadResults := list.New()
	var fileCount, byteCount, successCount, failCount int64
	for fileResult := range results {
		uploadResults.PushBack(fileResult)
		fileCount++
		byteCount += fileResult.Bytes
		if fileResult.Status == false {
			failCount++
		} else {
			successCount++
		}
		if *trace {
			fmt.Printf("Upload result: File %s, Bytes %v, Status %v, Duration %v\n", fileResult.FileName, fileResult.Bytes, fileResult.Status, fileResult.Duration)
		}
		if *progress {
			displayProgress(fileResult, startTime, byteCount, fileCount, totalBytes, totalFiles)
		}
	}
	endTime := time.Now().UnixNano()
	fmt.Println("")
	displayBasicStats((endTime - startTime), fileCount, successCount, failCount, byteCount)
	if *stats {
		displayFileStats(uploadResults)
	}

	fmt.Println("Cloud Copy Uploader Complete")
}

func displayProgress(fileResult clouduploader.UploadResults, startTime, byteCount, fileCount, totalBytes, totalFiles int64) {
	if byteCount > 0 {
		endTime := time.Now().UnixNano()
		duration := endTime - startTime
		durationMs := duration / 1000000
		durationSec := float64(durationMs) / float64(1000)
		bitCount := float64(byteCount) * 8
		rate := bitCount / durationSec

		percent := ((float64(byteCount) / float64(totalBytes)) * float64(100))
		fmt.Printf("\r%3.0f percent complete, rate %4.2f bps", percent, rate)
	}
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
		if fileData.Status == false {
			fmt.Printf("File %s Failure: %s\n", fileData.FileName, fileData.Err)
		}
	}
}

func displayBasicStats(duration, fileCount, successCount, failCount, byteCount int64) {
	durationMs := duration / 1000000
	durationSec := float64(durationMs) / float64(1000)
	bitCount := float64(byteCount) * 8
	rate := bitCount / durationSec
	if failCount > 0 {
		fmt.Printf("FAILED Total Files %v, Success %v Failed %v Rate %.2f bps, %v Bytes, Duration %v (ms)\n", fileCount, successCount, failCount, rate, byteCount, durationMs)
	} else {
		fmt.Printf("Uploaded Total Files %v, Rate %.2f bps, %v Bytes, Duration %v (ms)\n", fileCount, rate, byteCount, durationMs)
	}
}

func validateInput(source, container, platform string) error {
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
	if len(platform) == 0 {
		return errors.New("Cloud Storage must be specified < aws | azure >")
	}
	if strings.Compare(platform,"aws") != 0 && strings.Compare(platform,"azure") != 0 {
		return errors.New("Cloud Storage must be < aws | azure >")
	}
	return nil
}

func getAccountInfoEnv() (string, string) {
	accountName := os.Getenv("ACCOUNT_NAME")
	accountKey := os.Getenv("ACCOUNT_KEY")
	return accountName, accountKey
}

func getRegionFromEnv() string {
	return os.Getenv("AWS_REGION")
}

func errorAndExit(err error) {
	fmt.Printf("Cloud Copy exiting; %s\n", err.Error())
	os.Exit(1)
}

func validateRegion( region string ) error {
	var err error = nil
	if (len(region) == 0 ) {
		err = errors.New("Region must be specified in environment AWS_REGION when using S3")
	}
	return err
}

func validateAccountInfo(accountName, accountKey string) error {
	var err error = nil
	if len(accountName) == 0 || len(accountKey) == 0 {
		err = errors.New("Environment variable ACCOUNT_NAME/ACCOUNT_KEY not set")
	}
	return err
}