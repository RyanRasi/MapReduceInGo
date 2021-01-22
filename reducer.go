//Map Reduce In Go
//Student ID
//28842293

package main

import (
	"fmt"
	"sort"
	"strings"
)

func reducer(passengersoneachflight map[int]map[string]int, flightsfromeachairport map[int]map[string]int, totalNauticalMilesPerFlight map[int]map[string]string, totalNauticalMilesPerPassenger map[int]map[string]float64, flightsBasedOnID map[int]map[string]string, numberOfCPUs int) {

	//PASSENGERS ON EACH FLIGHT

	dictPassengersOnEachFlight := make(map[string]int) //Creates a dictionary map to store return value
	for i := 0; i < numberOfCPUs; i++ {                //For every dictionary recieved thus far
		for key, value := range passengersoneachflight[i] {
			dictPassengersOnEachFlight[key] = dictPassengersOnEachFlight[key] + value //Set the dictionary map key to the value plus the value again
		}
	}
	outputArray := ""
	for key, value := range dictPassengersOnEachFlight { //For every key and value...
		if key == "--" {
		} else {
			//Append the array with the key and value followed by a new line
			outputArray = outputArray + strings.Replace(key, "-", "        ", -1) + "        " + fmt.Sprint(value) + "\n"
		}
	}
	outputData(outputArray, "PassengersOnEachFlight", 1, "NONE") //Call output data function

	//FLIGHTS FROM EACH AIRPORT

	dictFlightsFromEachAirport := make(map[string]int) //Creates a dictionary map to store return value
	for i := 0; i < numberOfCPUs; i++ {                //For every dictionary recieved thus far
		for key, value := range flightsfromeachairport[i] {
			dictFlightsFromEachAirport[key] = dictFlightsFromEachAirport[key] + value //Set the dictionary map key to the value plus the value again
		}
	}

	outputArray = ""

	sortedDictFlightsFromEachAirport := make([]string, 0, len(dictFlightsFromEachAirport))
	for name := range dictFlightsFromEachAirport {
		sortedDictFlightsFromEachAirport = append(sortedDictFlightsFromEachAirport, name) //Appens each value to this array
	}

	sort.Slice(sortedDictFlightsFromEachAirport, func(i, j int) bool { //Sorts the array slice and returns a sorted dictionary by value
		return dictFlightsFromEachAirport[sortedDictFlightsFromEachAirport[i]] > dictFlightsFromEachAirport[sortedDictFlightsFromEachAirport[j]]
	})

	for _, value := range sortedDictFlightsFromEachAirport {
		if value == "--" {
		} else {
			whitespace := "" //Formats white space and adds spaces for easier human viewing
			for i := 0; i < 21-len(value); i++ {
				whitespace = whitespace + " "
			}
			outputArray = outputArray + strings.Replace(value, "-", "               ", -1) + "        " + whitespace + fmt.Sprint(dictFlightsFromEachAirport[value]-8) + "\n"
		}
	}
	outputData(outputArray, "FlightsFromEachAirport", 2, "NONE") //Calls output data function

	//NAUTICAL MILES AND PASSENGER WITH THE MOST MILES

	//Dictionaries for miles per flight and passenger
	dictTotalNauticalMilesPerFlight := make(map[string]string)
	dictTotalNauticalMilesPerPassenger := make(map[string]float64)

	for i := 0; i < numberOfCPUs; i++ { //For every dictionary recieved thus far
		for key, value := range totalNauticalMilesPerFlight[i] {
			dictTotalNauticalMilesPerFlight[key] = value
		}
	}
	for i := 0; i < numberOfCPUs; i++ { //For every dictionary recieved thus far
		for key, value := range totalNauticalMilesPerPassenger[i] {
			dictTotalNauticalMilesPerPassenger[key] = dictTotalNauticalMilesPerPassenger[key] + value
		}
	}

	outputArrayFlights := ""
	for key, value := range dictTotalNauticalMilesPerFlight {
		if key == "--" {
		} else {
			//Append the output flights string with the key and value of the miles per flight
			outputArrayFlights = outputArrayFlights + strings.Replace(key, "-", "        ", -1) + "        " + fmt.Sprint(value) + "\n"
		}
	}
	//SORT FUNCTION
	sortedDictTotalNauticalMilesPerPassenger := make([]string, 0, len(dictTotalNauticalMilesPerPassenger))
	for name := range dictTotalNauticalMilesPerPassenger {
		sortedDictTotalNauticalMilesPerPassenger = append(sortedDictTotalNauticalMilesPerPassenger, name)
	}

	sort.Slice(sortedDictTotalNauticalMilesPerPassenger, func(i, j int) bool { //Sorts the passengers mile tracker so that the passenger with the most miles can be found
		return dictTotalNauticalMilesPerPassenger[sortedDictTotalNauticalMilesPerPassenger[i]] > dictTotalNauticalMilesPerPassenger[sortedDictTotalNauticalMilesPerPassenger[j]]
	})
	//SORT FUNCTION END

	outputArrayPassengers := ""
	for _, value := range sortedDictTotalNauticalMilesPerPassenger {
		if value == "--" {
		} else {
			//Append the output passengers string with the key and value of the miles per passenger
			outputArrayPassengers = outputArrayPassengers + value + "                " + fmt.Sprint(dictTotalNauticalMilesPerPassenger[value]) + "\n"
		}
	}

	outputData(outputArrayFlights, "TotalNauticalMilesPerFlightAndPassenger", 3, outputArrayPassengers) //Call the output data function

	//FLIGHTS BASED ON THEIR ID NUMBER
	dictFlightsBasedOnID := make(map[string]string)
	for i := 0; i < numberOfCPUs; i++ { //For every dictionary recieved thus far
		for key, value := range flightsBasedOnID[i] {
			//Append the current dicitonary with the existing value and the value again
			dictFlightsBasedOnID[key] = dictFlightsBasedOnID[key] + value
		}
	}
	outputArray = ""
	for key, value := range dictFlightsBasedOnID {
		if key == "--" {
		} else {
			tempValue := fmt.Sprint(value)
			//Make the output array equal to itself plus formating and the value which is the passenger list
			outputArray = outputArray + strings.Replace(key, "|", "        ", -1) + "   " + strings.Replace(tempValue, "-", "", 1) + "\n"
		}
	}
	outputData(outputArray, "FlightsBasedOnID", 4, "NONE") //Call the output data function
}
