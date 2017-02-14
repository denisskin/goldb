package goldb

import (
	"log"
	"syscall"
)

func init() {
	// set limit open files in process
	if err := syscall.Setrlimit(syscall.RLIMIT_NOFILE, &syscall.Rlimit{999999, 999999}); err != nil {
		log.Println("!!!ERROR: syscall.Setrlimit-Error: ", err)
	}
}
