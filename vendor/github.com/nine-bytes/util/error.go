package util

import (
	"fmt"
)

type CodeError struct {
	Code int    `json:"code"`
	Text string `json:"text"`
}

func NewCodeError(code int, text string) *CodeError {
	return &CodeError{Code: code, Text: text}
}

func (ce *CodeError) Error() string {
	return fmt.Sprintf("error code: %d error text: %s", ce.Code, ce.Text)
}

// Runs the given function and converts any panic encountered while doing so
// into an error. Useful for sending to channels that will close
func PanicToError(fn func()) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic: %v", r)
		}
	}()
	fn()
	return
}
