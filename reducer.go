//Map Reduce In Go
//Student ID
//28842293

package main

import (
	"fmt"
	"sort"
	"strings"
)

func reducer(passengersoneachflight map[int]map[string]int, flightsfromeachairport map[int]map[string]int, totalNauticalMilesPerFlight map[int]map[string]string, totalNauticalMilesPerPassenger map[int]map[string]float64, numberOfCPUs int) {

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
	outputData(outputArray, "passengersoneachflight", 1, "NONE")

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
	outputData(outputArray, "flightsfromeachairport", 2, "NONE")

	//NAUTICAL MILES AND PASSENGER WITH THE MOST MILES

	dictTotalNauticalMilesPerFlight := make(map[string]string)
	dictTotalNauticalMilesPerPassenger := make(map[string]float64)
	//fmt.Println(totalNauticalMilesPerFlight)
	for i := 0; i < numberOfCPUs; i++ {
		for key, value := range totalNauticalMilesPerFlight[i] {
			dictTotalNauticalMilesPerFlight[key] = value
		}
	}
	for i := 0; i < numberOfCPUs; i++ {
		for key, value := range totalNauticalMilesPerPassenger[i] {
			dictTotalNauticalMilesPerPassenger[key] = dictTotalNauticalMilesPerPassenger[key] + value
		}
	}
	//fmt.Println(dictTotalNauticalMilesPerPassenger)

	outputArrayFlights := ""
	for key, value := range dictTotalNauticalMilesPerFlight {
		if key == "--" {
		} else {
			outputArrayFlights = outputArrayFlights + strings.Replace(key, "-", "        ", -1) + "        " + fmt.Sprint(value) + "\n"
		}
	}
	//SORT FUNCTION
	sortedDictTotalNauticalMilesPerPassenger := make([]string, 0, len(dictTotalNauticalMilesPerPassenger))
	for name := range dictTotalNauticalMilesPerPassenger {
		sortedDictTotalNauticalMilesPerPassenger = append(sortedDictTotalNauticalMilesPerPassenger, name)
	}

	sort.Slice(sortedDictTotalNauticalMilesPerPassenger, func(i, j int) bool {
		return dictTotalNauticalMilesPerPassenger[sortedDictTotalNauticalMilesPerPassenger[i]] > dictTotalNauticalMilesPerPassenger[sortedDictTotalNauticalMilesPerPassenger[j]]
	})
	//SORT FUNCTION END

	outputArrayPassengers := ""
	for _, value := range sortedDictTotalNauticalMilesPerPassenger {
		if value == "--" {
		} else {
			outputArrayPassengers = outputArrayPassengers + value + "                " + fmt.Sprint(dictTotalNauticalMilesPerPassenger[value]) + "\n"
		}
	}
	//fmt.Println(dictTotalNauticalMilesPerPassenger)

	outputData(outputArrayFlights, "totalnauticalmiles", 3, outputArrayPassengers)
}
