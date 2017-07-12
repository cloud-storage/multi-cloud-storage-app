package filelisting

import (
	"os"
	"path/filepath"
)

//FileHandle individual result for folder traversal
type FileHandle struct {
	FilePath string
	FileInfo os.FileInfo
	Err      error
}

//GetSizes return byte size and number of files for file/folder
func GetSizes(source string) (int64, int64, error) {
	var byteCount, fileCount int64
	fileInfo, err := getStat(source)
	if err != nil {
		return byteCount, fileCount, err
	}
	if fileInfo.IsDir() {
		byteCount, fileCount = getFolderCounters(source)
	} else {
		byteCount = fileInfo.Size()
		fileCount = 1
	}
	return byteCount, fileCount, nil
}

//ListFiles traverse folders returning individual items
func ListFiles(source string) chan FileHandle {
	fileProcessing := make(chan FileHandle)
	go traverseFiles(source, fileProcessing)
	return fileProcessing
}

func traverseFiles(source string, results chan FileHandle) {
	filepath.Walk(source, func(path string, fileInfo os.FileInfo, err error) error {
		results <- FileHandle{FilePath: path, FileInfo: fileInfo, Err: err}
		return nil
	})
	close(results)
}

func getStat(source string) (os.FileInfo, error) {
	return os.Stat(source)
}

func getFolderCounters(source string) (int64, int64) {
	var byteCount, fileCount int64
	filepath.Walk(source, func(path string, fileInfo os.FileInfo, err error) error {
		if !fileInfo.IsDir() {
			fileCount++
			byteCount += fileInfo.Size()
		}
		return nil
	})
	return byteCount, fileCount
}
