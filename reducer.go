//Map Reduce In Go
//Student ID
//28842293

package main

import (
	"fmt"
	"strings"
)

func reducer(passengersoneachflight map[int]map[string]int, flightsfromeachairport map[int]map[string]int, numberOfCPUs int) {

	//PASSENGERS ON EACH FLIGHT
	dictPassengersOnEachFlight := make(map[string]int)
	for i := 0; i < numberOfCPUs; i++ {
		for key, value := range passengersoneachflight[i] {
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
	outputData(outputArray, "passengersoneachflight", 1)

	//FLIGHTS FROM EACH AIRPORT
	dictFlightsFromEachAirport := make(map[string]int)
	for i := 0; i < numberOfCPUs; i++ {
		for key, value := range flightsfromeachairport[i] {
			dictFlightsFromEachAirport[key] = dictFlightsFromEachAirport[key] + value
		}
	}
	outputArray = ""
	for key, value := range dictFlightsFromEachAirport {
		if key == "--" {
		} else {
			outputArray = outputArray + strings.Replace(key, "-", "        ", -1) + "        " + fmt.Sprint(value) + "\n"
		}
	}
	outputData(outputArray, "flightsfromeachairport", 2)
}
