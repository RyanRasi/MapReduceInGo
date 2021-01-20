//Map Reduce In Go
//Student ID
//28842293

package main

import (
	"fmt"
	"sort"
	"strings"
)

func reducer(passengersoneachflight map[int]map[string]int, flightsfromeachairport map[int]map[string]int, totalNauticalMiles map[int]map[string]string, numberOfCPUs int) {

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

	sortedDictFlightsFromEachAirport := make([]string, 0, len(dictFlightsFromEachAirport))
	for name := range dictFlightsFromEachAirport {
		sortedDictFlightsFromEachAirport = append(sortedDictFlightsFromEachAirport, name)
	}

	sort.Slice(sortedDictFlightsFromEachAirport, func(i, j int) bool {
		return dictFlightsFromEachAirport[sortedDictFlightsFromEachAirport[i]] > dictFlightsFromEachAirport[sortedDictFlightsFromEachAirport[j]]
	})

	for _, value := range sortedDictFlightsFromEachAirport {
		//fmt.Printf("%-7v %v\n", value, dictFlightsFromEachAirport[value])
		if value == "--" {
		} else {
			whitespace := ""

			for i := 0; i < 21-len(value); i++ {
				whitespace = whitespace + " "
			}
			outputArray = outputArray + strings.Replace(value, "-", "               ", -1) + "        " + whitespace + fmt.Sprint(dictFlightsFromEachAirport[value]-8) + "\n"
		}
	}
	outputData(outputArray, "flightsfromeachairport", 2)

	//NAUTICAL MILES AND PASSENGER WITH THE MOST MILES
	dictTotalNauticalMiles := make(map[string]string)
	//fmt.Println(totalNauticalMiles)
	for i := 0; i < numberOfCPUs; i++ {
		for key, value := range totalNauticalMiles[i] {
			dictTotalNauticalMiles[key] = value
		}
	}
	fmt.Println(dictTotalNauticalMiles)

	outputArray = ""
	for key, value := range dictTotalNauticalMiles {
		if key == "--" {
		} else {
			outputArray = outputArray + strings.Replace(key, "-", "        ", -1) + "        " + fmt.Sprint(value) + "\n"
		}
	}
	outputData(outputArray, "totalnauticalmiles", 3)
}
