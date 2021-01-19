//Map Reduce In Go
//Student ID
//28842293

package main

import (
	"fmt"
	"strings"
)

func reducer(input1 map[string]int, input2 map[string]int, input3 map[string]int, input4 map[string]int, input5 map[string]int, input6 map[string]int, input7 map[string]int, input8 map[string]int) {
	dictPassengersOnEachFlight := make(map[string]int)

	for key, value := range input1 {
		dictPassengersOnEachFlight[key] = dictPassengersOnEachFlight[key] + value
		//Probs if statement
	}
	for key, value := range input2 {
		dictPassengersOnEachFlight[key] = dictPassengersOnEachFlight[key] + value
		//Probs if statement
	}
	for key, value := range input3 {
		dictPassengersOnEachFlight[key] = dictPassengersOnEachFlight[key] + value
		//Probs if statement
	}
	for key, value := range input4 {
		dictPassengersOnEachFlight[key] = dictPassengersOnEachFlight[key] + value
		//Probs if statement
	}
	for key, value := range input5 {
		dictPassengersOnEachFlight[key] = dictPassengersOnEachFlight[key] + value
		//Probs if statement
	}
	for key, value := range input6 {
		dictPassengersOnEachFlight[key] = dictPassengersOnEachFlight[key] + value
		//Probs if statement
	}
	for key, value := range input7 {
		dictPassengersOnEachFlight[key] = dictPassengersOnEachFlight[key] + value
		//Probs if statement
	}
	for key, value := range input8 {
		dictPassengersOnEachFlight[key] = dictPassengersOnEachFlight[key] + value
		//Probs if statement
	}
	outputArray := ""
	for key, value := range dictPassengersOnEachFlight {
		if key == "--" {
		} else {
			outputArray = outputArray + strings.Replace(key, "-", "    ", -1) + "    " + fmt.Sprint(value) + "\n"
		}
	}
	//fmt.Println(outputArray)
	outputData(outputArray, "passengersoneachflight", 1)
}
