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

	noCPUS := runtime.NumCPU() //Calculates Current CPUs
	fmt.Println("Processors Available: ", noCPUS)

	baseLines := 0
	additionalLinesNeeded := 0
	currentLineSelected := 0
	placeholderText := "EMPTY,EMPTY,EMPTY,EMPTY,EMPTY,EMPTY"

	//Opens file and reads contents
	file, err := os.Open("./data/real/AComp_Passenger_data.csv")

	if err != nil {
		log.Fatal(err)
	}

	defer file.Close()

	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		baseLines++
		//fmt.Println(scanner.Text())
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
	//Closes file
	fmt.Println("Number of lines is: ", baseLines)
	fmt.Println("Lines/Processors: ", baseLines/noCPUS)

	countersRequired := baseLines //Calculates additional lines required to make cpu divisable by lines

	if countersRequired%noCPUS != 0 { //If there are lines required then...
		for {
			countersRequired++                //Increase line variable
			if countersRequired%noCPUS == 0 { //If theyare equal then...
				fmt.Println("Additional Lines and number of lines is ", countersRequired)
				fmt.Println("Lines/Processors: ", countersRequired/noCPUS)
				break
			}
		}
		additionalLinesNeeded = countersRequired - baseLines               //Take the additional lines from the starting to find out how many needed.
		fmt.Println("Additional Counters needed: ", additionalLinesNeeded) // Output to console
	}

	processorAllocatorArray := make([][]string, noCPUS) //Initialises the allocation of lines to processors
	for i := range processorAllocatorArray {
		processorAllocatorArray[i] = make([]string, (countersRequired / noCPUS)) //Make an array for the amount of lines needed / cpu - 49 in this case
	}
	for i := 0; i < len(processorAllocatorArray); i++ { //For however long this array is, 8 in this case, then...
		for j := 0; j < len(processorAllocatorArray[i]); j++ { //For however long the lines needed per processor is then...
			currentLineSelected++ //Sets the row counter
			lastLineRead := 0     //Sets the last line read
			//Opens file and reads contents
			file, err := os.Open("./data/real/AComp_Passenger_data.csv") //Opens the CSV file
			if err != nil {                                              //If there is an error then log it
				log.Fatal(err)
			}
			defer file.Close() //Closes file

			scanner := bufio.NewScanner(file) //Scans each line. This block of code makes sure that the correct rows are allocated to the correct CPU
			for scanner.Scan() {              //For every line...
				fmt.Println(scanner.Text())
				lastLineRead++                           //Increase the last line read
				if lastLineRead == currentLineSelected { //If the line equals the row then send the row to the correct part of the array and to upper case
					processorAllocatorArray[i][j] = strings.ToUpper(scanner.Text())
				}
				//Error handling
				if (additionalLinesNeeded != 0) && (currentLineSelected > baseLines) { //If additional lines are needed for the processor then the placeholder text is inserted instead
					processorAllocatorArray[i][j] = strings.ToUpper(placeholderText)
				}
				if scanner.Text() == "" { //If the row is empty then the placeholder text is inserted instead
					processorAllocatorArray[i][j] = strings.ToUpper(placeholderText)
				}
				if len(scanner.Text()) < 35 { //If the length of the row is less than 35 then the placeholder text is inserted instead
					processorAllocatorArray[i][j] = strings.ToUpper(placeholderText)
				}
			}

		}
	}
	//fmt.Println(processorAllocatorArray)
	//Processor allocator array contains ALL OF THE DATA
	//Buffer One Test
	processorSelector := 0                                              //	currentCPU := 1
	currentBufferArray := make([][]string, (countersRequired / noCPUS)) //Current buffer array is just the current processor
	for i := range currentBufferArray {
		currentBufferArray[i] = make([]string, 6) //Array of 6 as that is how many fields there are for the rows
	}

	for i := 0; i < len(currentBufferArray); i++ { //For every 49...
		tempBufferArray := strings.Split(processorAllocatorArray[processorSelector][i], ",") //Split the string rows into individual components
		for j := 0; j < len(tempBufferArray); j++ {                                          //For the length of the array then...
			currentBufferArray[i][j] = tempBufferArray[j] //Set the temp array to the current buffer e.g. [49][6]
		}
	}
	//fmt.Println("Current Buffer Array: ", currentBufferArray) //Prints the current buffer

	//Flights from each airport - Main part - Counts the number of flights
	dictFlights := make(map[string]int)
	for i := 0; i < len(currentBufferArray); i++ {

		flights := strings.Fields(currentBufferArray[i][2])

		for _, flight := range flights {
			dictFlights[flight]++
		}
	}
	//fmt.Println(dictFlights)

	//Passengers on each flight-------------------------------------
	dictPassengersOnEachFlight := make(map[string]int)
	for i := 0; i < len(currentBufferArray); i++ {
		//	currentBufferArray[i][2]
		//}
		concatonatePassengerFlights := currentBufferArray[i][1] + "-" + currentBufferArray[i][2] + "-" + currentBufferArray[i][3]
		passengerFlights := strings.Fields(concatonatePassengerFlights)
		for _, flightID := range passengerFlights {
			dictPassengersOnEachFlight[flightID]++
			//Probs if statement
		}
	}
	//fmt.Println("Current Buffer Array", currentBufferArray) //Prints the lines currently assigned to the processor
	//
	fmt.Println(dictPassengersOnEachFlight)
	textOutput := ""
	var s string
	for key, val := range dictPassengersOnEachFlight {
		// Convert each key/value pair in m to a string
		s = s + fmt.Sprintf("%s=\"%s", key, val) + "\n"
	}
	textOutput = strings.Replace(s, ")", "", -1)
	textOutput = strings.Replace(textOutput, "=\"%!s(int=", "          ", -1) //Formats text to replace characters with whitespace
	textOutput = strings.Replace(textOutput, "-", "          ", -1)           //Formats text to replace characters with whitespace

	outputFile("outputPassengersOnEachFlight", textOutput, 2) // Calls output to file function for flights from each airport task
	//Passenger flights end-----------------------------------------

	//Call airport list open text file
	var airportList [30][5]string
	file, err = os.Open("./data/real/Top30_airports_LatLong.csv") //Opens file
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()
	scanner = bufio.NewScanner(file)
	airportListCounter := 0 //Sets a counter for each airport
	for scanner.Scan() {
		if scanner.Text() == "" { //Handles empty rows
			continue
		}
		tempSplit := strings.Split(scanner.Text(), ",") //Splits data by commas
		for i := 0; i < len(tempSplit); i++ {
			if len(tempSplit[0]) < 21 { //If the string is less than 21 characters then a whitespace is added for formatting
				whiteSpace := 21 - len(tempSplit[0])
				for i := 0; i < whiteSpace; i++ {
					tempSplit[0] = tempSplit[0] + " "
				}
			}
			airportList[airportListCounter][0] = (tempSplit[1] + "               " + tempSplit[0]) // Combines the Code and the Airport in one variable
			airportList[airportListCounter][i+1] = tempSplit[i]                                    // Shifts all the exisiting variables one column up forthe variable above
		}
		airportListCounter++
	}
	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
	//
	for i := 0; i < len(airportList); i++ { //Adds all the airports as one to be taken off the count later which will act as the "empty flights"
		flights := strings.Fields(airportList[i][2])
		for _, flight := range flights {
			dictFlights[flight]++
		}
	}

	replacementDictFlights := make(map[string]int)
	for k, v := range dictFlights {
		for i := 0; i < len(airportList); i++ {
			if strings.Contains(airportList[i][0], k) { //Renmaes just the airport code to the airport airportName with the code
				replacementDictFlights[airportList[i][0]] = v
			}
		}
	}
	dictFlights = replacementDictFlights
	for k := range dictFlights {
		dictFlights[k]--
	}

	fmt.Println("/////////////////////////////////////////////////////////////")

	airports := make([]string, 0, len(dictFlights))
	for airportName := range dictFlights {
		airports = append(airports, airportName)
	}
	sort.Slice(airports, func(i, j int) bool {
		return dictFlights[airports[i]] > dictFlights[airports[j]]
	})
	textOutput = ""

	for _, airportName := range airports {
		//fmt.Printf("%-7v %v\n", airportName, dictFlights[airportName])
		textOutput = textOutput + airportName + fmt.Sprintf("%d", dictFlights[airportName]) + "\n"
	}
	outputFile("outputFlightsFromAirport", textOutput, 1) // Calls output to file function for flights from each airport task
}
func flightsFromEachAirport(data string) {

}
func inputFile(fileID string) {
}
func outputFile(fileID string, textOutput string, outputFormat int) {

	//Write to txt file, flights from each airport
	file, err := os.Create(fileID + ".txt")
	if err != nil {
		return
	}
	defer file.Close()
	if outputFormat == 1 {
		file.WriteString("IATA/FAA Code:    " + "Airport:     " + "        Flights: " + "\n" + textOutput)
	} else if outputFormat == 2 {
		file.WriteString("Flight Number:    " + "Depart:      " + "Arrival:     " + "Passengers on Flight: " + "\n" + textOutput)
	} else if outputFormat == 3 {
		file.WriteString("Flight Number:    " + "Depart:      " + "Arrival:     " + "Miles(Nautical): " + "\n" + textOutput)
	}
}
