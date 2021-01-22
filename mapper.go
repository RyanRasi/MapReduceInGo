//Map Reduce In Go
//Student ID
//28842293

package main

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
)

func mapper(processorInput [][]string, airportData [30][4]string, task1Channel chan map[string]int, task2Channel chan map[string]int, task3aChannel chan map[string]string, task3bChannel chan map[string]float64, task4Channel chan map[string]string) {

	//PASSENGERS ON EACH FLIGHT

	counterPassengersonEachFlight := make(map[string]int) //Counter map which keeps track of the passenegrs on each flight
	data := make([]string, len(processorInput))           //Makes an array for however big the data is
	for i := 0; i < len(processorInput); i++ {
		data[i] = processorInput[i][1] + "-" + processorInput[i][2] + "-" + processorInput[i][3] //Concatonates the flight ID and departure and arrival airports
	}
	for i := 0; i < len(data); i++ { //For however long the data is...
		if data[i] == "" { //If there is no data, then skip
			break
		} else {
			counterPassengersonEachFlight[data[i]]++ //Increment the occurances of each flight
		}
	}
	task1Channel <- counterPassengersonEachFlight //Return the result to the main function so that the reducer can be called

	//NUMBER OF FLIGHTS FROM EACH AIRPORT

	counterFlightsFromEachAirport := make(map[string]int) //Counter map which keeps track of the flights from each airport
	//data := make([]string, len(processorInput))           //Makes an array for however big the data is
	tempString := ""
	for i := 0; i < len(processorInput); i++ { //For however long the data is...
		for j := 0; j < len(airportData); j++ {
			if airportData[j][1] == processorInput[i][2] {
				tempString = processorInput[i][2] + "-" + airportData[j][0] //Sets the temp string as the processor and irport data flight codes and airport
			}
		}
		if tempString == "" {
		} else {
			counterFlightsFromEachAirport[tempString]++ //Increment the occurances of each flight
		}
	}
	//This adds counts each airport from the airport data file so that they can be deducted to find airports that were 0
	for i := 0; i < len(airportData); i++ {
		tempString = airportData[i][1] + "-" + airportData[i][0]
		if tempString == "" {
		} else {
			counterFlightsFromEachAirport[tempString]++ //Increment the occurances of each flight
		}
	}

	task2Channel <- counterFlightsFromEachAirport //Return the result to the main function so that the reducer can be called

	//NAUTICAL MILES PER FLIGHT AND TOTAL FOR EACH PASSENGER

	flightMiles := make(map[string]string)                       //Counts the flight miles per flight
	flightMileTracker := make([][]string, (len(processorInput))) //Stores all of the miles
	passengerMileTracker := make(map[string]float64)             //Counts the passenger miles per flight

	for i := range flightMileTracker {
		flightMileTracker[i] = make([]string, 9) //Array of 9 stores the data from airport data as well as the lat and long calculations
	}
	for i := 0; i < len(processorInput); i++ { //Creates a new array with the flight name, abbreviated airport codes and the lat and long for both to and from airports
		if processorInput[i][2] == "" {
			continue
		}
		flightMileTracker[i][0] = processorInput[i][1] //Sets the data to the input flight name, and depart and arrival airports
		flightMileTracker[i][1] = processorInput[i][2]
		flightMileTracker[i][2] = processorInput[i][3]
		if processorInput[i][2] != "" {
			for j := 0; j < len(airportData); j++ {
				if strings.Contains(airportData[j][1], flightMileTracker[i][1]) { //If the airport codes match then...
					flightMileTracker[i][3] = airportData[j][2] //Add the lat and long for the departure ariport
					flightMileTracker[i][4] = airportData[j][3]
				}
				if strings.Contains(airportData[j][1], flightMileTracker[i][2]) {
					flightMileTracker[i][5] = airportData[j][2] //Add the lat and long for the departure ariport
					flightMileTracker[i][6] = airportData[j][3]
				}
			}
		}
		flightMileTracker[i][8] = flightMileTracker[i][0] + "-" + flightMileTracker[i][1] + "-" + flightMileTracker[i][2]
	}
	for i := 0; i < len(flightMileTracker); i++ {
		//Converts lat and long to float64
		lat1, _ := strconv.ParseFloat(flightMileTracker[i][3], 64)
		lng1, _ := strconv.ParseFloat(flightMileTracker[i][4], 64)
		lat2, _ := strconv.ParseFloat(flightMileTracker[i][5], 64)
		lng2, _ := strconv.ParseFloat(flightMileTracker[i][6], 64)

		nauticalMiles := distance(lat1, lng1, lat2, lng2, "N") //Distance function called

		flightMileTracker[i][7] = fmt.Sprint(nauticalMiles) //Converts value to string

		miles := strings.Fields(flightMileTracker[i][8])

		for _, mile := range miles { //Adds the result per flight number to the dictionary map
			flightMiles[mile] = flightMileTracker[i][7]
		}
	}

	//PASSENGER MILEAGE CALCULATOR
	passengerIDFlightsAndMiles := make([]string, len(processorInput)) //Makes array to allow for cross-reference of the flight miles map and

	for i := 0; i < len(processorInput); i++ {
		for key, value := range flightMiles {
			tempSplit := strings.Split(key, "-")      //Split the results from the flight miles
			if tempSplit[0] == processorInput[i][1] { //If they match up with the passenger array then...
				passengerIDFlightsAndMiles[i] = (processorInput[i][0] + "-" + value) //Set the new array to these values
			}
		}
	}
	for i := 0; i < len(passengerIDFlightsAndMiles); i++ {
		tempSplit := strings.Split(passengerIDFlightsAndMiles[i], "-")
		if tempSplit[0] == "" { //If there is no data, then skip
			break
		} else {
			tempFloat, _ := strconv.ParseFloat(tempSplit[1], 64)
			passengerMileTracker[tempSplit[0]] = passengerMileTracker[tempSplit[0]] + tempFloat //Increment the occurances of each flight
		}

	}

	task3aChannel <- flightMiles          //Return the result to the main function so that the reducer can be called
	task3bChannel <- passengerMileTracker //Return the result to the main function so that the reducer can be called

	// LIST OF FLIGHTS BASED ON FLIGHT ID
	flightsBasedOnID := make(map[string]string)

	for i := 0; i < len(processorInput); i++ {
		var tempArray [6]string
		if processorInput[i][4] == "" {
		} else {
			tempArray[0] = processorInput[i][1]                              // Sets the FlightID
			tempArray[1] = processorInput[i][2] + "-" + processorInput[i][3] // Sets the IATA/FAA Code

			departureTime, err := strconv.ParseInt(processorInput[i][4], 10, 64) //Sets the departure time to the input
			if err != nil {
				panic(err)
			}
			departureTimeSplit := strings.Split(time.Unix(departureTime, 0).String(), " ") //Splits the return value so that the string is just the hours and minutes
			tempArray[2] = departureTimeSplit[1]                                           // Unix Epoch conversion to proper format

			flightTimeInput, err := strconv.ParseInt(processorInput[i][5], 10, 64)
			arrivalTime := departureTime + (flightTimeInput * 60) //Finds arrival time by multiplying flight time which is in minutes to find the seconds then adding the departure time
			if err != nil {
				panic(err)
			}
			arrivalTimeSplit := strings.Split(time.Unix(arrivalTime, 0).String(), " ") //Splits by a space so that the hours and minutes are obtaines
			tempArray[3] = arrivalTimeSplit[1]                                         // Unix Epoch conversion to proper format

			flightTime := strings.Split(time.Unix(flightTimeInput, 0).String(), " ") //Converts flight time and splits string

			hoursMinutesSeconds := strings.Split(flightTime[1], ":") // Splits to gain just the hours and minutes
			tempArray[4] = ("Hours: " + hoursMinutesSeconds[1] + " - Minutes: " + hoursMinutesSeconds[2])
			flightEntry := tempArray[0] + "|" + tempArray[1] + "|" + tempArray[2] + "|" + tempArray[3] + "|" + tempArray[4] //Formats text for the reducer

			flightsBasedOnID[flightEntry] = flightsBasedOnID[flightEntry] + "-" + processorInput[i][0] //Splits passenger ID entries

		}
	}
	task4Channel <- flightsBasedOnID //Returns back to main job for the reducer to be called
}
func distance(lat1 float64, lng1 float64, lat2 float64, lng2 float64, unit ...string) float64 {
	//Distance function source from - https://www.geodatasource.com/developers/go
	const PI float64 = 3.141592653589793

	radlat1 := float64(PI * lat1 / 180)
	radlat2 := float64(PI * lat2 / 180)

	theta := float64(lng1 - lng2)
	radtheta := float64(PI * theta / 180)

	dist := math.Sin(radlat1)*math.Sin(radlat2) + math.Cos(radlat1)*math.Cos(radlat2)*math.Cos(radtheta)

	if dist > 1 {
		dist = 1
	}

	dist = math.Acos(dist)
	dist = dist * 180 / PI
	dist = dist * 60 * 1.1515

	if len(unit) > 0 {
		if unit[0] == "K" {
			dist = dist * 1.609344
		} else if unit[0] == "N" {
			dist = dist * 0.8684
		}
	}

	return dist
}
