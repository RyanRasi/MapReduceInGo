package main

import (
	"fmt"
	"runtime"
)

func main() {

	noCPUS := runtime.NumCPU()
	fmt.Println(noCPUS)

}
