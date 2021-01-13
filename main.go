package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"runtime"
	"strings"
)

func main() {

	noCPUS := runtime.NumCPU()
	fmt.Println(noCPUS)
	baseCounter := 0
	additionalCountersNeeded := 0
	currentRowSelected := 0
	placeholderText := "EMPTY,EMPTY,EMPTY,EMPTY,EMPTY,EMPTY"
	var airportList [30][5]string

	//Opens file and reads contents
	file, err := os.Open("./data/test/AComp_Passenger_data_no_error.csv")

	if err != nil {
		log.Fatal(err)
	}

	defer file.Close()

	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		baseCounter++
		//fmt.Println(scanner.Text())
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
	//Closes file
	fmt.Println("Number of lines is: ", baseCounter)
	fmt.Println("Lines/Processors: ", baseCounter/noCPUS)
	fmt.Println("Lines/Processors: ", baseCounter%noCPUS == 0)

	countersRequired := baseCounter

	if countersRequired%noCPUS != 0 {
		for {
			countersRequired++
			if countersRequired%noCPUS == 0 {
				fmt.Println("Additional Lines and number of lines is ", countersRequired)
				fmt.Println("Lines/Processors: ", countersRequired/noCPUS)
				break
			}
		}

		additionalCountersNeeded = countersRequired - baseCounter
		fmt.Println("Additional Counters needed: ", additionalCountersNeeded)
	}

	processorAllocatorArray := make([][]string, noCPUS)
	for i := range processorAllocatorArray {
		processorAllocatorArray[i] = make([]string, (countersRequired / noCPUS))
		//for j := range processorAllocatorArray[i] {
		//	processorAllocatorArray[i][j] = make([]string, 6)
		//}
	}

	for i := 0; i < len(processorAllocatorArray); i++ {
		for j := 0; j < len(processorAllocatorArray[i]); j++ {

			currentRowSelected++
			lastLineRead := 0
			//
			//Opens file and reads contents
			file, err := os.Open("./data/test/AComp_Passenger_data_no_error.csv")

			if err != nil {
				log.Fatal(err)
			}

			defer file.Close()

			scanner := bufio.NewScanner(file)

			for scanner.Scan() {
				lastLineRead++
				if lastLineRead == currentRowSelected {
					processorAllocatorArray[i][j] = strings.ToUpper(scanner.Text())
				}
				if (additionalCountersNeeded != 0) && (currentRowSelected > baseCounter) {
					processorAllocatorArray[i][j] = strings.ToUpper(placeholderText)
				}
				if scanner.Text() == "" {
					processorAllocatorArray[i][j] = strings.ToUpper(placeholderText)
				}
				if len(scanner.Text()) < 35 {
					processorAllocatorArray[i][j] = strings.ToUpper(placeholderText)
				}
			}

		}
	}
	//fmt.Println(processorAllocatorArray[0][0]) //For testing the output
	//End of Block function
	//Creating output files

	flightsFromEachAirport := "\nHello World!"

	file, err = os.Create("outputFlightsFromAirport.txt")
	if err != nil {
		return
	}
	defer file.Close()

	file.WriteString("IATA/FAA Code:    " + "Airport:     " + "Flights from each airport:     " + flightsFromEachAirport)

	//Buffer One Test
	//	currentCPU := 1
	currentBufferArray := make([][]string, (countersRequired / noCPUS))
	for i := range currentBufferArray {
		currentBufferArray[i] = make([]string, 6)
	}

	for i := 0; i < len(currentBufferArray); i++ {
		bufferArray := strings.Split(processorAllocatorArray[0][i], ",")
		for j := 0; j < len(bufferArray); j++ {
			currentBufferArray[i][j] = bufferArray[j]
		}
	}
	fmt.Println(currentBufferArray[0])

	//Flights from each airport [2]
	dictFlights := make(map[string]int)
	for i := 0; i < len(currentBufferArray); i++ {
		//	currentBufferArray[i][2]
		//}

		flights := strings.Fields(currentBufferArray[i][2])

		for _, flight := range flights {
			dictFlights[flight]++
			//Probs if statement
		}
	}
	fmt.Println(dictFlights)
	//Passengers on each flight-------------------------------------
	dictPassengersonFlight := make(map[string]int)
	for i := 0; i < len(currentBufferArray); i++ {
		//	currentBufferArray[i][2]
		//}
		passengerFlights := strings.Fields(currentBufferArray[i][1])
		for _, flightID := range passengerFlights {
			dictPassengersonFlight[flightID]++
			//Probs if statement
		}
	}
	fmt.Println(dictPassengersonFlight)
	//Read from top 30 airports

	//Opens file and reads contents
	file, err = os.Open("./data/real/Top30_airports_LatLong.csv")

	if err != nil {
		log.Fatal(err)
	}

	defer file.Close()

	scanner = bufio.NewScanner(file)
	airportListCounter := 0
	for scanner.Scan() {
		if scanner.Text() == "" {
			continue
		}
		tempSplit := strings.Split(scanner.Text(), ",")

		//baseCounter++
		//fmt.Println("TempList is: ", tempSplit)
		for i := 0; i < len(tempSplit); i++ {
			airportList[airportListCounter][0] = (tempSplit[1] + "    " + tempSplit[0]) // Combines the Code and the Airport in one variable
			airportList[airportListCounter][i+1] = tempSplit[i]
		}
		airportListCounter++
	}

	//fmt.Println(airportList)

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
	//Closes file
}
