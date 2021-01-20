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
		}
		counterPassengersonEachFlight[data[i]]++ //Increment the occurances of each flight
	}
	//return counterPassengersonEachFlight
	task1Channel <- counterPassengersonEachFlight //Return the result to the main function so that the reducer can be called

	//NUMBER OF FLIGHTS FROM EACH AIRPORT
	counterFlightsFromEachAirport := make(map[string]int) //Counter map which keeps track of the flights from each airport
	//data := make([]string, len(processorInput))           //Makes an array for however big the data is
	tempString := ""
	for i := 0; i < len(processorInput); i++ { //For however long the data is...
		for j := 0; j < len(airportData); j++ {
			if airportData[j][1] == processorInput[i][2] {
				tempString = processorInput[i][2] + "-" + airportData[j][0]
			}
		}
		if tempString == "" {
		} else {
			counterFlightsFromEachAirport[tempString]++ //Increment the occurances of each flight
		}
	}
	for i := 0; i < len(airportData); i++ {
		tempString = airportData[i][1] + "-" + airportData[i][0]
		if tempString == "" {
		} else {
			counterFlightsFromEachAirport[tempString]++ //Increment the occurances of each flight
		}
	}

	task2Channel <- counterFlightsFromEachAirport //Return the result to the main function so that the reducer can be called

	//NAUTICAL MILES PER FLIGHT AND TOTAL FOR EACH PASSENGER

	flightMiles := make(map[string]string)
	flightMileTracker := make([][]string, (len(processorInput))) //Current buffer array is just the current processor
	//passengerMiles := make(map[string]string)
	passengerMileTracker := make(map[string]float64)
	for i := range flightMileTracker {
		flightMileTracker[i] = make([]string, 9) //Array of 6 as that is how many fields there are for the rows
	}
	for i := 0; i < len(processorInput); i++ { //Creates a new array with the flight name, abbreviated airport codes and the lat and long for both to and from airports
		if processorInput[i][2] == "" {
			continue
		}
		flightMileTracker[i][0] = processorInput[i][1]
		flightMileTracker[i][1] = processorInput[i][2]
		flightMileTracker[i][2] = processorInput[i][3]
		if processorInput[i][2] != "" {
			for j := 0; j < len(airportData); j++ {
				if strings.Contains(airportData[j][1], flightMileTracker[i][1]) {
					flightMileTracker[i][3] = airportData[j][2]
					flightMileTracker[i][4] = airportData[j][3]
				}
				if strings.Contains(airportData[j][1], flightMileTracker[i][2]) {
					flightMileTracker[i][5] = airportData[j][2]
					flightMileTracker[i][6] = airportData[j][3]
				}
				//}
			}
		}
		flightMileTracker[i][8] = flightMileTracker[i][0] + "-" + flightMileTracker[i][1] + "-" + flightMileTracker[i][2]
	}
	//fmt.Println(flightMileTracker)
	for i := 0; i < len(flightMileTracker); i++ {
		lat1, _ := strconv.ParseFloat(flightMileTracker[i][3], 64)
		lng1, _ := strconv.ParseFloat(flightMileTracker[i][4], 64)
		lat2, _ := strconv.ParseFloat(flightMileTracker[i][5], 64)
		lng2, _ := strconv.ParseFloat(flightMileTracker[i][6], 64)

		nauticalMiles := distance(lat1, lng1, lat2, lng2, "N")

		flightMileTracker[i][7] = fmt.Sprint(nauticalMiles)
		//fmt.Println(flightMileTracker)

		miles := strings.Fields(flightMileTracker[i][8])

		for _, mile := range miles {
			flightMiles[mile] = flightMileTracker[i][7]
		}
	}

	//PASSENGER MILEAGE CALCULATOR
	passengerIDFlightsAndMiles := make([]string, len(processorInput))

	for i := 0; i < len(processorInput); i++ {
		for key, value := range flightMiles {
			//fmt.Println(processorInput[i][0])
			tempSplit := strings.Split(key, "-")
			if tempSplit[0] == processorInput[i][1] {
				passengerIDFlightsAndMiles[i] = (processorInput[i][0] + "-" + value)
			}
		}
	}
	for i := 0; i < len(passengerIDFlightsAndMiles); i++ {
		//fmt.Println(passengerIDFlightsAndMiles[i])
	}
	//fmt.Println(passengerIDFlightsAndMiles)
	for i := 0; i < len(passengerIDFlightsAndMiles); i++ {
		tempSplit := strings.Split(passengerIDFlightsAndMiles[i], "-")
		if tempSplit[0] == "" { //If there is no data, then skip
			break
		} else {
			//fmt.Println(tempSplit[1])
			tempFloat, _ := strconv.ParseFloat(tempSplit[1], 64)
			passengerMileTracker[tempSplit[0]] = passengerMileTracker[tempSplit[0]] + tempFloat //Increment the occurances of each flight
		}

	}
	//	fmt.Println(passengerMileTracker)

	task3aChannel <- flightMiles //Return the result to the main function so that the reducer can be called
	task3bChannel <- passengerMileTracker

	// LIST OF FLIGHTS BASED ON FLIGHT ID
	flightsBasedOnID := make(map[string]string)

	for i := 0; i < len(processorInput); i++ {
		var tempArray [6]string
		if processorInput[i][4] == "" {
		} else {
			tempArray[0] = processorInput[i][1]                              //Sets the FlightID
			tempArray[1] = processorInput[i][2] + "-" + processorInput[i][3] // Sets the IATA/FAA Code

			departureTime, err := strconv.ParseInt(processorInput[i][4], 10, 64)
			if err != nil {
				panic(err)
			}
			departureTimeSplit := strings.Split(time.Unix(departureTime, 0).String(), " ")
			tempArray[2] = departureTimeSplit[1] // Unix Epoch conversion to proper format
			//
			//flightTimeInput, err := strconv.ParseInt(processorInput[i][4], 10, 64)
			flightTimeInput, err := strconv.ParseInt(processorInput[i][5], 10, 64)
			arrivalTime := departureTime + (flightTimeInput * 60)
			if err != nil {
				panic(err)
			}
			arrivalTimeSplit := strings.Split(time.Unix(arrivalTime, 0).String(), " ")
			tempArray[3] = arrivalTimeSplit[1] // Unix Epoch conversion to proper format

			flightTime := strings.Split(time.Unix(flightTimeInput, 0).String(), " ")

			hoursMinutesSeconds := strings.Split(flightTime[1], ":")
			tempArray[4] = ("Hours: " + hoursMinutesSeconds[1] + " - Minutes: " + hoursMinutesSeconds[2])
			flightEntry := tempArray[0] + "|" + tempArray[1] + "|" + tempArray[2] + "|" + tempArray[3] + "|" + tempArray[4]

			flightsBasedOnID[flightEntry] = flightsBasedOnID[flightEntry] + "-" + processorInput[i][0] //Splits passenger ID entries

		}
	}
	//fmt.Println(flightsBasedOnID)
	task4Channel <- flightsBasedOnID
}
func distance(lat1 float64, lng1 float64, lat2 float64, lng2 float64, unit ...string) float64 {
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
