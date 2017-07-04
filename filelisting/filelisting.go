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
