package util

import (
	"os"
	"fmt"
	"path/filepath"
	"strings"
	"runtime"
)

var executableFile string
var executableDirectory string

func AddExecutableDirectory(filename string) string {
	if runtime.GOOS == "windows" {
		filename = strings.Replace(filename, "/", "\\", -1)
		return fmt.Sprintf("%s\\%s", executableDirectory, filename)
	}

	filename = strings.Replace(filename, "\\", "/", -1)
	return fmt.Sprintf("%s/%s", executableDirectory, filename)
}

func ExcutableFile() string {
	return executableFile
}

func ExcutableDirectory() string {
	return executableDirectory
}

func FilesInRoot(root string) (files []string, err error) {
	err = filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if !info.IsDir() {
			if files == nil {
				files = make([]string, 0)
			}

			files = append(files, path)
		}

		return nil
	})

	return
}
