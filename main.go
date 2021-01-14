package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"runtime"
	"sort"
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
	fmt.Println("Current Buffer Array", currentBufferArray)
	fmt.Println("Dictionary of Passenger flights", dictPassengersonFlight)

	//Call airport list open text file

	for i := 0; i < len(airportList); i++ { //Adds all the airports as one to be taken off the count later which will act as the "empty flights"
		flights := strings.Fields(airportList[i][2])
		for _, flight := range flights {
			dictFlights[flight]++
		}
	}

	replacementDictFlights := make(map[string]int)
	for k, v := range dictFlights {
		for i := 0; i < len(airportList); i++ {
			if strings.Contains(airportList[i][0], k) {
				replacementDictFlights[airportList[i][0]] = v
			}
		}
	}
	dictFlights = replacementDictFlights
	for k := range dictFlights {
		dictFlights[k]--
	}

	fmt.Println("/////////////////////////////////////////////////////////////")

	names := make([]string, 0, len(dictFlights))
	for name := range dictFlights {
		names = append(names, name)
	}
	fmt.Println(names)
	fmt.Println()

	sort.Slice(names, func(i, j int) bool {
		return dictFlights[names[i]] > dictFlights[names[j]]
	})
	fmt.Println(names)
	fmt.Println()
	textOutput := ""

	for _, name := range names {
		//fmt.Printf("%-7v %v\n", name, dictFlights[name])
		textOutput = textOutput + name + fmt.Sprintf("%d", dictFlights[name]) + "\n"
	}

	//fmt.Println(textOutput)

	//fmt.Println(dictFlights)
	//replacementDictFlights := make(map[string]int)

	//var textOutput string
	outputFile("outputFlightsFromAirport", textOutput) // Calls output to file function for flights from each airport task

}
func flightsFromEachAirport(data string) {

}
func inputFile(fileID string, taskID string) {

}
func outputFile(fileID string, textOutput string) {

	//Write to txt file, flights from each airport
	file, err := os.Create(fileID + ".txt")
	if err != nil {
		return
	}
	defer file.Close()
	file.WriteString("IATA/FAA Code:    " + "Airport:     " + "        Flights: " + "\n" + textOutput)
}
