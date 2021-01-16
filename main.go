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
	"sort"
	"strings"
)

func main() {

	noCPUS := runtime.NumCPU() //Calculates Current CPUs
	//Prints Processors Available
	fmt.Println("Processors Available: ", noCPUS)

	//Declares and Initiliases Empty Variables
	baseLines := 0
	//additionalLinesNeeded := 0
	totalLines := 0
	currentLineSelected := 0
	//placeholderText := "EMPTY,EMPTY,EMPTY,EMPTY,EMPTY,EMPTY"
	passengerDataPath := "./data/real/AComp_Passenger_data.csv"
	re := regexp.MustCompile(`^[a-zA-Z0-9,]*$`)
	unknownEntries := ""
	//airportDataPath := "./data/real/Top30_airports_LatLong.csv"

	//Opens main passenger data file
	file, err := os.Open(passengerDataPath)
	if err != nil { //If there is an error then log the error
		log.Fatal(err)
	}
	defer file.Close()
	scanner := bufio.NewScanner(file) //For every file, increment the base lines variable
	for scanner.Scan() {
		if len(scanner.Text()) > 15 { //Error handling - If the text length is a low number then it is not counted in the variable
			match := re.FindStringSubmatch(scanner.Text()) //REGEX to get rid of symbols and characters that are not a letter or number
			if len(match) != 0 {
				baseLines++
			}
		}
		totalLines++
	}
	if err := scanner.Err(); err != nil { //Log error if there is one
		log.Fatal(err)
	}
	fmt.Println("Total Lines: ", baseLines)
	//Closes file
	extraLinesNeeded := checkLinesPerProcessor(baseLines, noCPUS)

	fmt.Println("Lines allocated per processor: ", (baseLines+extraLinesNeeded)/noCPUS)
	//Sets up allocating array for the processor
	processorAllocatedLines := make([][]string, noCPUS) //Initialises the allocation of lines to processors
	for i := range processorAllocatedLines {
		processorAllocatedLines[i] = make([]string, ((baseLines + extraLinesNeeded) / noCPUS)) //Make a 2D array for the amount of lines needed
	}
	for i := 0; i < len(processorAllocatedLines); i++ { //For however long this array is based on the CPU amount then...
		for j := 0; j < len(processorAllocatedLines[i]); j++ { //For however long the lines needed per processor is then...
			currentLineSelected++ //Sets the row counter, text file starts on 1
			lastLineRead := 0     //Sets the last line read
			//Opens file and reads contents
			file, err := os.Open(passengerDataPath) //Opens the CSV file
			if err != nil {                         //If there is an error then log it
				log.Fatal(err)
			}
			defer file.Close() //Closes file

			scanner := bufio.NewScanner(file) //Scans each line. This block of code makes sure that the correct rows are allocated to the correct CPU
			for scanner.Scan() {              //For every line...
				lastLineRead++ //Increase the last line read
				if lastLineRead == currentLineSelected {
					if len(scanner.Text()) > 15 { //Error handling, if data is smaller than 15 then it is discarded
						match := re.FindStringSubmatch(scanner.Text()) //REGEX to get rid of symbols and characters that are not a letter or number
						if len(match) != 0 {
							processorAllocatedLines[i][j] = strings.ToUpper(scanner.Text())
						} else {
							unknownEntries = unknownEntries + scanner.Text() + "\n"
							j = j - 1
						}
					} else {
						unknownEntries = unknownEntries + scanner.Text() + "\n"
						j = j - 1
					}
				}
				//if () {
				//	extraLinesNeeded = extraLinesNeeded - 1
				//	processorAllocatedLines[i][j] = strings.ToUpper(placeholderText)
				//}
				//rowTracker =
			}
		}
	}
	outputFile("outputPassengerErrorDataEntries", unknownEntries, 0) // Calls output to file function for passenger data entries with an error
	//----fmt.Println(processorAllocatedLines)

	//FOR PROCESSOR ONE---------------------------------------------------
	processorOne := 0
	processorOneDataArray := make([][]string, ((baseLines + extraLinesNeeded) / noCPUS)) //Current buffer array is just the current processor
	for i := range processorOneDataArray {
		processorOneDataArray[i] = make([]string, 6) //Array of 6 as that is how many fields there are for the rows
	}

	for i := 0; i < len(processorOneDataArray); i++ { //For every 49...
		tempBufferArray := strings.Split(processorAllocatedLines[processorOne][i], ",") //Split the string rows into individual components
		for j := 0; j < len(tempBufferArray); j++ {                                     //For the length of the array then...
			processorOneDataArray[i][j] = tempBufferArray[j] //Set the temp array to the current buffer e.g. [49][6]
		}
	}
	//Calls passengers on each flight
	passengersOnEachFlight(processorOneDataArray)
	//Calls flights from each airport
	flightsFromEachAirport(processorOneDataArray)
}

func inputFile(fileID string) {

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
		additionalLinesNeeded = additionalLines - lines                 //Take the additional lines from the starting to find out how many needed.
		fmt.Println("Additional Lines needed: ", additionalLinesNeeded) // Output to console
	}
	return additionalLinesNeeded
}
func outputFile(fileID string, textOutput string, outputFormat int) {
	//Write to txt file, flights from each airport
	file, err := os.Create("./output/" + fileID + ".txt")
	if err != nil {
		return
	}
	defer file.Close()
	if outputFormat == 0 {
		file.WriteString("Passenger Data Entries with Errors Below: " + "\n\n" + textOutput)
	} else if outputFormat == 1 {
		file.WriteString("Flight Number:    " + "Depart:      " + "Arrival:     " + "Passengers on Flight: " + "\n" + textOutput)
	} else if outputFormat == 2 {
		file.WriteString("IATA/FAA Code:    " + "Airport:     " + "        Flights: " + "\n" + textOutput)
	}
}
func passengersOnEachFlight(processorArray [][]string) {
	dictPassengersOnEachFlight := make(map[string]int)
	for i := 0; i < len(processorArray); i++ {
		//	processorArray[i][2]
		//}
		concatonatePassengerFlights := processorArray[i][1] + "-" + processorArray[i][2] + "-" + processorArray[i][3]
		passengerFlights := strings.Fields(concatonatePassengerFlights)
		for _, flightID := range passengerFlights {
			dictPassengersOnEachFlight[flightID]++
			//Probs if statement
		}
	}
	//fmt.Println("Current Buffer Array", processorArray) //Prints the lines currently assigned to the processor
	//
	//-----fmt.Println(dictPassengersOnEachFlight)
	textOutput := ""
	var s string
	for key, val := range dictPassengersOnEachFlight {
		// Convert each key/value pair in m to a string
		s = s + fmt.Sprintf("%s=\"%s", key, val) + "\n"
	}
	textOutput = strings.Replace(s, ")", "", -1)
	textOutput = strings.Replace(textOutput, "=\"%!s(int=", "          ", -1) //Formats text to replace characters with whitespace
	textOutput = strings.Replace(textOutput, "-", "          ", -1)           //Formats text to replace characters with whitespace
	outputFile("outputPassengersOnEachFlight", textOutput, 1)                 // Calls output to file function for flights from each airport task

}
func flightsFromEachAirport(processorArray [][]string) {
	//Flights from each airport - Main part - Counts the number of flights
	dictFlights := make(map[string]int)
	for i := 0; i < len(processorArray); i++ {

		flights := strings.Fields(processorArray[i][2])

		for _, flight := range flights {
			dictFlights[flight]++
		}
	}
	//Call airport list open text file
	var airportList [30][5]string
	file, err := os.Open("./data/real/Top30_airports_LatLong.csv") //Opens file
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
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
			if strings.Contains(airportList[i][0], k) { //Renames just the airport code to the airport airportName with the code
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
	textOutput := ""

	for _, airportName := range airports {
		//fmt.Printf("%-7v %v\n", airportName, dictFlights[airportName])
		textOutput = textOutput + airportName + fmt.Sprintf("%d", dictFlights[airportName]) + "\n"
	}
	outputFile("outputFlightsFromAirport", textOutput, 2) // Calls output to file function for flights from each airport task
}
