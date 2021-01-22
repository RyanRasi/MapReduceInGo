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
	"time"
)

func main() {
	start := time.Now() //Starts time elapsed variable

	numberofCPUs := runtime.NumCPU() //Calculates Current CPUs

	//An array of maps are created to hold all of the values from the mapper and pass them through to the reducer
	passengersOnEachFlightMapArray := make(map[int]map[string]int)
	flightsFromEachAirportMapArray := make(map[int]map[string]int)
	totalNauticalMilesPerFlightArray := make(map[int]map[string]string)
	totalNauticalMilesPerPassengerArray := make(map[int]map[string]float64)
	flightsBasedOnID := make(map[int]map[string]string)

	fmt.Println("Processors Available: ", numberofCPUs) //Prints available processors

	//
	baseLines := 0 //Total lines in the text file
	lines := 0     //Total correct lines in the text file

	//File name for inputs
	passengerDataPath := "./data/real/AComp_Passenger_data.csv"
	airportDataPath := "./data/real/Top30_airports_LatLong.csv"

	//REGEX Statement for Error detection on passenger data
	rePassengerData := regexp.MustCompile(`^[a-zA-Z]{3}[0-9]{4}[a-zA-Z]{2}[0-9][,][a-zA-Z]{3}[0-9]{4}[a-zA-Z][,][a-zA-Z]{3}[,][a-zA-Z]{3},[0-9]{10}[,][0-9]{1,4}`)
	unknownData := ""             //Unknown data entries variable
	var airportData [30][4]string //Holds all of the data from the top 30 airports file

	//Opens top 30 airports data file
	file, err := os.Open(airportDataPath)
	fmt.Println("MapReduce - Reading from Data Source")
	if err != nil { //If there is an error then log the error
		log.Fatal(err)
	}
	defer file.Close()
	scanner := bufio.NewScanner(file) //For every file, increment the base lines variable
	currentLineinAirportData := 0     //Current line in file
	for scanner.Scan() {
		if scanner.Text() == "" { //Handles empty rows
			continue
		} else {
			tempSplit := strings.Split(scanner.Text(), ",") //Splits data by commas
			//Stores array
			airportData[currentLineinAirportData][0] = tempSplit[0]
			airportData[currentLineinAirportData][1] = tempSplit[1]
			airportData[currentLineinAirportData][2] = tempSplit[2]
			airportData[currentLineinAirportData][3] = tempSplit[3]
		}
		if err := scanner.Err(); err != nil { //Log error if there is one
			log.Fatal(err)
		}
		currentLineinAirportData++ //Increments line
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
		if len(match) != 0 {                                        //If there is a match then...
			tempSplit := strings.Split(strings.ToUpper(scanner.Text()), ",") //Split the input by a comma
			for i := 0; i < len(airportData); i++ {                          //For every record in the airport data...
				if tempSplit[2] == airportData[i][1] { //If the IATA/FAA code from the passenger entries and from airport data matches...
					for i := 0; i < len(airportData); i++ {
						if tempSplit[3] == airportData[i][1] { //Do the comparison again for the arrival airport
							lines++ //Increment lines
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
			processorAllocatedLines[i][j] = make([]string, 6) //3rd part of array is each data field
		}
	}
	//Opens main passenger data file
	file, err = os.Open(passengerDataPath)
	if err != nil { //If there is an error then log the error
		log.Fatal(err)
	}
	defer file.Close()
	scanner = bufio.NewScanner(file) //For every file, increment the base lines variable
	currentCPU := 0                  //Determines where each input should go to
	currentLine := 0                 //Same as above
	for scanner.Scan() {
		match := rePassengerData.FindStringSubmatch(scanner.Text()) //REGEX to get rid of incorrect data entries
		if len(match) != 0 {
			tempSplit := strings.Split(strings.ToUpper(scanner.Text()), ",") //Splits input by a comma
			for i := 0; i < len(airportData); i++ {
				if tempSplit[2] == airportData[i][1] { //Checks if IATA/FAA code matches
					for i := 0; i < len(airportData); i++ {
						if tempSplit[3] == airportData[i][1] { //Sets each line to their processor allocation number
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
			unknownData = unknownData + scanner.Text() + "\n" //If REGEX failed then data is appended to unknown data variable
		}
		//baseLines++
		if err := scanner.Err(); err != nil { //Log error if there is one
			log.Fatal(err)
		}
	}
	//Closes File

	outputData(unknownData, "unknownDataEntries", 0, "NONE") //Calls the output data text file for the unknown entries

	//Channels are the processors alerting the main job that the task has been completed
	task1Channel := make(chan map[string]int)      //Passengers on each flight
	task2Channel := make(chan map[string]int)      //Flights from each airport
	task3aChannel := make(chan map[string]string)  //Total Nautical Miles - Per Flight
	task3bChannel := make(chan map[string]float64) //Total Nautical Miles - Per Passenger
	task4Channel := make(chan map[string]string)   //Flights based on the ID number

	// SENDS DATA TO THE MAPPER
	fmt.Println("MapReduce - Map and Reduce Task Started")
	for i := 0; i < numberofCPUs; i++ { //For every CPU do...
		//Opens concurrent processor and calls mapper with that processors specific input
		go mapper(processorAllocatedLines[i], airportData, task1Channel, task2Channel, task3aChannel, task3bChannel, task4Channel)

		//Sets channel return value to the dictionary map
		passengersOnEachFlightMapArray[i] = <-task1Channel
		flightsFromEachAirportMapArray[i] = <-task2Channel
		totalNauticalMilesPerFlightArray[i] = <-task3aChannel
		totalNauticalMilesPerPassengerArray[i] = <-task3bChannel
		flightsBasedOnID[i] = <-task4Channel

		//SENDS THE DATA TO THE REDUCER
		//Opens concurrent processor and calls reducer
		go reducer(passengersOnEachFlightMapArray, flightsFromEachAirportMapArray, totalNauticalMilesPerFlightArray, totalNauticalMilesPerPassengerArray, flightsBasedOnID, numberofCPUs)
	}
	fmt.Println("MapReduce - Map and Reduce Task Finished")

	t := time.Now()
	elapsed := t.Sub(start) //Outputs the time elapsed
	fmt.Println("MapReduce - Time Elapsed: ", elapsed)
}

func checkLinesPerProcessor(lines int, cpus int) int {
	//To make sure that there are enough lines for each processor array then this block is ran...
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
