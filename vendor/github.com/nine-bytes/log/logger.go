package log

import (
	"fmt"
	"log"
	"runtime"
	"strings"
	"io"
	"os"
	"github.com/nine-bytes/util"
)

var logger = log.New(os.Stderr, "", log.LstdFlags)
var quiet = false

func SetDefault(w io.Writer, q bool) {
	logger.SetOutput(w)
	quiet = q
}

func pfx(level, fmtstr string) string {
	var shortFile string

	fpcs := make([]uintptr, 1)
	// Skip 3 levels to get the caller
	if runtime.Callers(3, fpcs) != 0 {
		if caller := runtime.FuncForPC(fpcs[0] - 1); caller != nil {
			// Print the file name and line number
			file, line := caller.FileLine(fpcs[0] - 1)
			shortFile = strings.TrimPrefix(fmt.Sprintf("%s line %d", file, line), util.ExcutableDirectory())
		}
	}

	return fmt.Sprintf("%s: [%s] %s\n", shortFile, level, fmtstr)
}

func Debug(arg0 string, args ...interface{}) {
	if !quiet {
		logger.Printf(pfx("DEBUG", arg0), args...)
	}
}

func Info(arg0 string, args ...interface{}) {
	logger.Printf(pfx("INFO", arg0), args...)
}

func Error(arg0 string, args ...interface{}) error {
	logger.Printf(pfx("ERROR", arg0), args...)
	return fmt.Errorf(arg0, args...)
}

func Fatal(arg0 string, args ...interface{}) {
	logger.Fatalf(pfx("FATAL", arg0), args...)
}
