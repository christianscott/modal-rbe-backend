package main

import (
	"fmt"
	"os"
	"runtime"
)

func main() {
	host, _ := os.Hostname()
	fmt.Printf("hello from %s — built for %s/%s\n", host, runtime.GOOS, runtime.GOARCH)
}
