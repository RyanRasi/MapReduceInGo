//Map Reduce In Go
//Student ID
//28842293

package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"regexp"
	"runtime"
	"strings"
)

func main() {
	numberofCPUs := runtime.NumCPU() //Calculates Current CPUs
	passengersOnEachFlightMapArray := make(map[int]map[string]int)
	flightsFromEachAirportMapArray := make(map[int]map[string]int)
	totalNauticalMilesPerFlightArray := make(map[int]map[string]string)
	totalNauticalMilesPerPassengerArray := make(map[int]map[string]float64)

	fmt.Println("Processors Available: ", numberofCPUs)
	baseLines := 0
	lines := 0

	passengerDataPath := "./data/real/AComp_Passenger_data.csv"
	airportDataPath := "./data/real/Top30_airports_LatLong.csv"

	rePassengerData := regexp.MustCompile(`^[a-zA-Z]{3}[0-9]{4}[a-zA-Z]{2}[0-9][,][a-zA-Z]{3}[0-9]{4}[a-zA-Z][,][a-zA-Z]{3}[,][a-zA-Z]{3},[0-9]{10}[,][0-9]{1,4}`)
	unknownData := ""
	var airportData [30][4]string

	//Opens top 30 airports data file
	file, err := os.Open(airportDataPath)
	if err != nil { //If there is an error then log the error
		log.Fatal(err)
	}
	defer file.Close()
	scanner := bufio.NewScanner(file) //For every file, increment the base lines variable
	currentLineinAirportData := 0
	for scanner.Scan() {
		if scanner.Text() == "" { //Handles empty rows
			continue
		} else {
			tempSplit := strings.Split(scanner.Text(), ",") //Splits data by commas
			airportData[currentLineinAirportData][0] = tempSplit[0]
			airportData[currentLineinAirportData][1] = tempSplit[1]
			airportData[currentLineinAirportData][2] = tempSplit[2]
			airportData[currentLineinAirportData][3] = tempSplit[3]
		}
		if err := scanner.Err(); err != nil { //Log error if there is one
			log.Fatal(err)
		}
		currentLineinAirportData++
	}

	//Opens main passenger data file
	file, err = os.Open(passengerDataPath)
	if err != nil { //If there is an error then log the error
		log.Fatal(err)
	}
	defer file.Close()
	scanner = bufio.NewScanner(file) //For every file, increment the base lines variable
	for scanner.Scan() {
		match := rePassengerData.FindStringSubmatch(scanner.Text()) //REGEX to get rid of incorrect data entries
		if len(match) != 0 {
			tempSplit := strings.Split(strings.ToUpper(scanner.Text()), ",")
			for i := 0; i < len(airportData); i++ {
				if tempSplit[2] == airportData[i][1] {
					for i := 0; i < len(airportData); i++ {
						if tempSplit[3] == airportData[i][1] {
							lines++
							break
						}
					}
				}
			}

		}
		baseLines++
		if err := scanner.Err(); err != nil { //Log error if there is one
			log.Fatal(err)
		}
	}
	//Closes file

	fmt.Println("Total Lines in file: ", baseLines)                                        // Counts total lines from text file
	fmt.Println("Total Correct Lines: ", lines)                                            // Counts total correct lines with error detection
	additionalLinesNeeded := checkLinesPerProcessor(lines, numberofCPUs)                   // Checks lines so that they are exactly divisable
	fmt.Println("Lines Per Processor: ", ((lines + additionalLinesNeeded) / numberofCPUs)) // Prints to console the ammount of lines per processor

	//fmt.Println(len(airportData))

	processorAllocatedLines := make([][][]string, numberofCPUs) //Initialises the allocation of lines to processors
	for i := range processorAllocatedLines {
		processorAllocatedLines[i] = make([][]string, ((lines + additionalLinesNeeded) / numberofCPUs)) //Make a 2D array for the amount of lines needed
		for j := range processorAllocatedLines[i] {
			processorAllocatedLines[i][j] = make([]string, 6)
		}
	}
	//Opens main passenger data file
	file, err = os.Open(passengerDataPath)
	if err != nil { //If there is an error then log the error
		log.Fatal(err)
	}
	defer file.Close()
	scanner = bufio.NewScanner(file) //For every file, increment the base lines variable
	currentCPU := 0
	currentLine := 0
	for scanner.Scan() {
		match := rePassengerData.FindStringSubmatch(scanner.Text()) //REGEX to get rid of incorrect data entries
		if len(match) != 0 {
			tempSplit := strings.Split(strings.ToUpper(scanner.Text()), ",")
			for i := 0; i < len(airportData); i++ {
				if tempSplit[2] == airportData[i][1] {
					for i := 0; i < len(airportData); i++ {
						if tempSplit[3] == airportData[i][1] {
							processorAllocatedLines[currentCPU][currentLine][0] = tempSplit[0]
							processorAllocatedLines[currentCPU][currentLine][1] = tempSplit[1]
							processorAllocatedLines[currentCPU][currentLine][2] = tempSplit[2]
							processorAllocatedLines[currentCPU][currentLine][3] = tempSplit[3]
							processorAllocatedLines[currentCPU][currentLine][4] = tempSplit[4]
							processorAllocatedLines[currentCPU][currentLine][5] = tempSplit[5]
							currentLine++
							if currentLine == ((lines + additionalLinesNeeded) / numberofCPUs) {
								currentCPU++
								currentLine = 0
							}
							break
						}
					}
				}
			}

		} else {
			unknownData = unknownData + scanner.Text() + "\n"
		}
		//baseLines++
		if err := scanner.Err(); err != nil { //Log error if there is one
			log.Fatal(err)
		}
	}
	//Closes File

	outputData(unknownData, "unknownDataEntries", 0, "NONE")

	task1Channel := make(chan map[string]int)      //Passengers on each flight
	task2Channel := make(chan map[string]int)      //Flights from each airport
	task3aChannel := make(chan map[string]string)  //Total Nautical Miles - Per Flight
	task3bChannel := make(chan map[string]float64) //Total Nautical Miles - Per Passenger

	// SENDS DATA TO THE MAPPER
	for i := 0; i < numberofCPUs; i++ {
		go mapper(processorAllocatedLines[i], airportData, task1Channel, task2Channel, task3aChannel, task3bChannel)
		passengersOnEachFlightMapArray[i] = <-task1Channel
		flightsFromEachAirportMapArray[i] = <-task2Channel
		totalNauticalMilesPerFlightArray[i] = <-task3aChannel
		totalNauticalMilesPerPassengerArray[i] = <-task3bChannel
		//SENDS THE DATA TO THE REDUCER
	}
	reducer(passengersOnEachFlightMapArray, flightsFromEachAirportMapArray, totalNauticalMilesPerFlightArray, totalNauticalMilesPerPassengerArray, numberofCPUs)
}

func checkLinesPerProcessor(lines int, cpus int) int {
	//To make sure that there are enough lines for each processor then this block is ran...
	additionalLines := lines
	additionalLinesNeeded := 0
	if lines%cpus != 0 { //If there are lines required then...
		for {
			additionalLines++              //Increase line variable
			if additionalLines%cpus == 0 { //If they are equal then break the loop
				break
			}
		}
		additionalLinesNeeded = additionalLines - lines //Take the additional lines from the starting to find out how many needed.
	}
	return additionalLinesNeeded
}
