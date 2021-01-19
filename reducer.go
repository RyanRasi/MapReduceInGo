//Map Reduce In Go
//Student ID
//28842293

package main

import (
	"fmt"
	"strings"
)

func reducer(inputData map[int]map[string]int, numberOfCPUs int) {
	//PASSENGERS ON EACH FLIGHT
	dictPassengersOnEachFlight := make(map[string]int)
	for i := 0; i < numberOfCPUs; i++ {
		for key, value := range inputData[i] {
			dictPassengersOnEachFlight[key] = dictPassengersOnEachFlight[key] + value
		}
	}
	outputArray := ""
	for key, value := range dictPassengersOnEachFlight {
		if key == "--" {
		} else {
			outputArray = outputArray + strings.Replace(key, "-", "        ", -1) + "        " + fmt.Sprint(value) + "\n"
		}
	}
	//fmt.Println(outputArray)
	outputData(outputArray, "passengersoneachflight", 1)
}
