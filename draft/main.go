//Map Reduce In Go
//Student ID
//28842293

package main

import (
	"bufio"
	"fmt"
	"log"
	"math"
	"os"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
)

func main() {

	noCPUS := runtime.NumCPU() //Calculates Current CPUs
	//Prints Processors Available
	fmt.Println("Processors Available: ", noCPUS)
	flightMiles := make(map[string]string)
	passengerMiles := make(map[string]string)
	dictionaryPasengersonEachFlight := make(map[string]int)
	dictionaryFlightsFromEachAirport := make(map[string]int)
	//Declares and Initiliases Empty Variables
	baseLines := 0
	//additionalLinesNeeded := 0
	totalLines := 0
	currentLineSelected := 0
	//placeholderText := "EMPTY,EMPTY,EMPTY,EMPTY,EMPTY,EMPTY"
	passengerDataPath := "./data/real/AComp_Passenger_data.csv"
	re := regexp.MustCompile(`^[a-zA-Z0-9,]*$`)
	unknownEntries := ""
	var airportCode []string
	airportDataPath := "./data/real/Top30_airports_LatLong.csv"
	//Opens top 30 airports data file
	file, err := os.Open(airportDataPath)
	if err != nil { //If there is an error then log the error
		log.Fatal(err)
	}
	defer file.Close()
	scanner := bufio.NewScanner(file) //For every file, increment the base lines variable
	for scanner.Scan() {
		if scanner.Text() == "" { //Handles empty rows
			continue
		}
		tempSplit := strings.Split(scanner.Text(), ",") //Splits data by commas
		airportCode = append(airportCode, tempSplit[1]) //appends the airport code to array
		if err := scanner.Err(); err != nil {           //Log error if there is one
			log.Fatal(err)
		}
	}

	//Opens main passenger data file
	file, err = os.Open(passengerDataPath)
	if err != nil { //If there is an error then log the error
		log.Fatal(err)
	}
	defer file.Close()
	scanner = bufio.NewScanner(file) //For every file, increment the base lines variable
	for scanner.Scan() {
		if len(scanner.Text()) > 15 { //Error handling - If the text length is a low number then it is not counted in the variable
			match := re.FindStringSubmatch(scanner.Text()) //REGEX to get rid of symbols and characters that are not a letter or number
			if len(match) != 0 {
				tempSplit := strings.Split(scanner.Text(), ",")
				for i := 0; i < len(airportCode); i++ {
					if airportCode[i] == tempSplit[2] {
						for j := 0; j < len(airportCode); j++ {
							if airportCode[j] == tempSplit[3] {
								baseLines++
							}
						}
					}
				}
			}
		}
		totalLines++
		if err := scanner.Err(); err != nil { //Log error if there is one
			log.Fatal(err)
		}
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
							tempSplit := strings.Split(strings.ToUpper(scanner.Text()), ",")
							for l := 0; l < len(airportCode); l++ {
								//fmt.Println(airportCode[i])
								//fmt.Println(tempSplit)
								if airportCode[l] == tempSplit[2] {
									firstCodeCorrect := 0
									for k := 0; k < len(airportCode); k++ {
										if airportCode[k] == tempSplit[3] {
											firstCodeCorrect = 1
											processorAllocatedLines[i][j] = strings.ToUpper(scanner.Text())

										}
									}
									if firstCodeCorrect == 0 {
										unknownEntries = unknownEntries + scanner.Text() + " - Unknown IATA/FAA Code" + "\n"
										j = j - 1
									}
								}
							}
						} else {
							unknownEntries = unknownEntries + scanner.Text() + " - Incompatible Symbols in Data Entry" + "\n"
							j = j - 1
						}
					} else {
						unknownEntries = unknownEntries + scanner.Text() + " - Data Entry too Short" + "\n"
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
	outputFile("outputPassengerErrorDataEntries", unknownEntries, 0, "NONE") // Calls output to file function for passenger data entries with an error
	//fmt.Println(processorAllocatedLines[0])
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
	for i := 0; i < noCPUS; i++ {
		passengersOnEachFlight(processorAllocatedLines[i], dictionaryPasengersonEachFlight)
		flightsFromEachAirport(processorAllocatedLines[i], dictionaryFlightsFromEachAirport)
		flightAndPassengerMiles(processorAllocatedLines[i], airportDataPath, flightMiles, passengerMiles)

	}
	listOfFlights(processorAllocatedLines[0])
	//flightAndPassengerMiles(processorAllocatedLines[0], airportDataPath, flightMiles, passengerMiles)
	//passengersOnEachFlight(processorAllocatedLines[0], dictionaryPasengersonEachFlight)
	//passengersOnEachFlight(processorAllocatedLines[1], dictionaryPasengersonEachFlight)

	//
	//Calls flights from each airport
	//flightsFromEachAirport(processorAllocatedLines[0], dictionaryFlightsFromEachAirport)
	//Calls
	//flightsFromEachAirport(processorAllocatedLines[0], dictionaryFlightsFromEachAirport)
	//miles for each flight
	//total miles of each passenger
	//passenger with the most miles in order
	//flightAndPassengerMiles(processorOneDataArray, airportDataPath, flightMiles)
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
func outputFile(fileID string, textOutput string, outputFormat int, optionalArgument string) {
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
	} else if outputFormat == 3 {
		file.WriteString("Flight Number:  " + "Depart:    " + "Arrival:   " + "Nautical Miles: " + "\n" + textOutput + "\n" + "Passengers sorted by miles accrued: \n\n" + "Passenger Number:         Total Miles Flown:\n" + optionalArgument)
	}
}
func passengersOnEachFlight(processorArray []string, dictionaryPasengersonEachFlight map[string]int) {
	dictPassengersOnEachFlight := dictionaryPasengersonEachFlight

	currentProcessorDataArray := make([][]string, (len(processorArray))) //Current buffer array is just the current processor
	for i := range currentProcessorDataArray {
		currentProcessorDataArray[i] = make([]string, 6) //Array of 6 as that is how many fields there are for the rows
	}

	for i := 0; i < len(currentProcessorDataArray); i++ { //For every 49...
		tempBufferArray := strings.Split(processorArray[i], ",") //Split the string rows into individual components
		for j := 0; j < len(tempBufferArray); j++ {              //For the length of the array then...
			currentProcessorDataArray[i][j] = tempBufferArray[j] //Set the temp array to the current buffer e.g. [49][6]
		}
	}
	// End of string split
	for i := 0; i < len(currentProcessorDataArray); i++ {
		//	currentProcessorDataArray[i][2]
		if currentProcessorDataArray[i][2] != "" { //If the string is not empty then...
			concatonatePassengerFlights := currentProcessorDataArray[i][1] + "-" + currentProcessorDataArray[i][2] + "-" + currentProcessorDataArray[i][3]
			passengerFlights := strings.Fields(concatonatePassengerFlights)
			for _, flightID := range passengerFlights {
				dictPassengersOnEachFlight[flightID]++
				//Probs if statement
			}
		}
	}
	textOutput := ""
	var s string
	for key, val := range dictPassengersOnEachFlight {
		// Convert each key/value pair in m to a string
		s = s + fmt.Sprintf("%s=\"%s", key, val) + "\n"
	}
	textOutput = strings.Replace(s, ")", "", -1)
	textOutput = strings.Replace(textOutput, "=\"%!s(int=", "          ", -1) //Formats text to replace characters with whitespace
	textOutput = strings.Replace(textOutput, "-", "          ", -1)           //Formats text to replace characters with whitespace
	outputFile("outputPassengersOnEachFlight", textOutput, 1, "NONE")         // Calls output to file function for flights from each airport task

}
func flightsFromEachAirport(processorArray []string, dictionaryFlightsFromEachAirport map[string]int) {
	//Flights from each airport - Main part - Counts the number of flights
	currentProcessorDataArray := make([][]string, (len(processorArray))) //Current buffer array is just the current processor
	for i := range currentProcessorDataArray {
		currentProcessorDataArray[i] = make([]string, 6) //Array of 6 as that is how many fields there are for the rows
	}

	for i := 0; i < len(currentProcessorDataArray); i++ { //For every 49...
		tempBufferArray := strings.Split(processorArray[i], ",") //Split the string rows into individual components
		for j := 0; j < len(tempBufferArray); j++ {              //For the length of the array then...
			currentProcessorDataArray[i][j] = tempBufferArray[j] //Set the temp array to the current buffer e.g. [49][6]
		}
	}
	// End of string split
	//fmt.Println(currentProcessorDataArray)
	dictFlights := dictionaryFlightsFromEachAirport
	for i := 0; i < len(currentProcessorDataArray); i++ {

		flights := strings.Fields(currentProcessorDataArray[i][2])

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
		dictFlights[k] = dictFlights[k] - runtime.NumCPU() //Minus happens due to the amount of processers seemingly always incrementing by 1 every time
	}
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
	outputFile("outputFlightsFromAirport", textOutput, 2, "NONE") // Calls output to file function for flights from each airport task
}
func flightAndPassengerMiles(processorArray []string, airportDataPath string, flightMilesInput map[string]string, passengerMilesInput map[string]string) {
	//miles for each flight
	currentProcessorDataArray := make([][]string, (len(processorArray))) //Current buffer array is just the current processor
	for i := range currentProcessorDataArray {
		currentProcessorDataArray[i] = make([]string, 6) //Array of 6 as that is how many fields there are for the rows
	}

	for i := 0; i < len(currentProcessorDataArray); i++ { //For every 49...
		tempBufferArray := strings.Split(processorArray[i], ",") //Split the string rows into individual components
		for j := 0; j < len(tempBufferArray); j++ {              //For the length of the array then...
			currentProcessorDataArray[i][j] = tempBufferArray[j] //Set the temp array to the current buffer e.g. [49][6]
		}
	}
	// End of string split
	//total miles of each passenger
	//passenger with the most miles in order
	//flightMiles := make(map[string]string)
	flightMiles := flightMilesInput
	passengerMiles := passengerMilesInput
	passengerMileTracker := make(map[string]float64)
	flightMileTracker := make([][]string, (len(currentProcessorDataArray))) //Current buffer array is just the current processor
	for i := range flightMileTracker {
		flightMileTracker[i] = make([]string, 9) //Array of 6 as that is how many fields there are for the rows
	}
	//Sets up allocating array for the processor
	var airportMetadata [30][4]string
	file, err := os.Open(airportDataPath)
	if err != nil { //If there is an error then log the error
		log.Fatal(err)
	}
	defer file.Close()
	scanner := bufio.NewScanner(file) //For every file, increment the base lines variable
	line := 0
	for scanner.Scan() {
		if scanner.Text() == "" { //Handles empty rows
			continue
		}
		tempSplit := strings.Split(scanner.Text(), ",") //Splits data by commas
		for i := 0; i < len(tempSplit); i++ {
			airportMetadata[line][i] = tempSplit[i] //appends the airport code to array
			if err := scanner.Err(); err != nil {   //Log error if there is one
				log.Fatal(err)
			}
		}
		line++
	}
	//Closes top 30 airports file
	//fmt.Println(currentProcessorDataArray[0])
	for i := 0; i < len(currentProcessorDataArray); i++ { //Creates a new array with the flight name, abbreviated airport codes and the lat and long for both to and from airports
		if currentProcessorDataArray[i][2] == "" {
			continue
		}
		flightMileTracker[i][0] = currentProcessorDataArray[i][1]
		flightMileTracker[i][1] = currentProcessorDataArray[i][2]
		flightMileTracker[i][2] = currentProcessorDataArray[i][3]
		//flightMileTracker[i][2] = currentProcessorDataArray[3]
		//currentProcessorDataArray[i][0] = currentProcessorDataArray[i][1]
		if currentProcessorDataArray[i][2] != "" {
			for j := 0; j < len(airportMetadata); j++ {
				//for j := 0; 0 < len(airportMetadata[i]); j ++ {
				if strings.Contains(airportMetadata[j][1], flightMileTracker[i][1]) {
					flightMileTracker[i][3] = airportMetadata[j][2]
					flightMileTracker[i][4] = airportMetadata[j][3]
				}
				if strings.Contains(airportMetadata[j][1], flightMileTracker[i][2]) {
					flightMileTracker[i][5] = airportMetadata[j][2]
					flightMileTracker[i][6] = airportMetadata[j][3]
				}
				//}
			}
		}
		flightMileTracker[i][8] = flightMileTracker[i][0] + "-" + flightMileTracker[i][1] + "-" + flightMileTracker[i][2]
	}
	//fmt.Println(flightMileTracker)
	//fmt.Println(strconv.ParseFloat(flightMileTracker[0][3], 64))
	//var lat1 int64 = 0
	//if s, err := strconv.ParseFloat(flightMileTracker[0][3], 64); err == nil {
	//	fmt.Printf("%T, %v\n", s, s)
	//	lat1 = strconv.ParseInt(s)
	//}
	//fmt.Println(flightMileTracker[0])
	for i := 0; i < len(flightMileTracker); i++ {
		lat1, _ := strconv.ParseFloat(flightMileTracker[i][3], 64)
		lng1, _ := strconv.ParseFloat(flightMileTracker[i][4], 64)
		lat2, _ := strconv.ParseFloat(flightMileTracker[i][5], 64)
		lng2, _ := strconv.ParseFloat(flightMileTracker[i][6], 64)

		nauticalMiles := distance(lat1, lng1, lat2, lng2, "N")

		flightMileTracker[i][7] = fmt.Sprint(nauticalMiles)

		miles := strings.Fields(flightMileTracker[i][8])

		for _, mile := range miles {
			flightMiles[mile] = flightMileTracker[i][7]
		}

	}
	//Miles of each passenger

	for i := 0; i < len(currentProcessorDataArray); i++ { //Adds all the airports as one to be taken off the count later which will act as the "empty flights"
		passengers := strings.Fields(currentProcessorDataArray[i][0])
		for _, passenger := range passengers {
			passengerMiles[passenger] = passengerMiles[passenger] + currentProcessorDataArray[i][1] + ","
		}
	}

	for passenger, flightName := range passengerMiles {
		tempSplitPassengerFlights := strings.Split(flightName, ",")
		for i := 0; i < len(tempSplitPassengerFlights); i++ {
			for key, value := range flightMiles {
				tempSplitFlightNames := strings.Split(key, "-")
				if tempSplitPassengerFlights[i] == tempSplitFlightNames[0] {
					floatValue, _ := strconv.ParseFloat(value, 64)
					passengerMileTracker[passenger] = passengerMileTracker[passenger] + floatValue
				}
			}
		}
	}
	//fmt.Println(passengerMileTracker)
	//fmt.Println(currentProcessorDataArray[0][0])
	//fmt.Println(passengerMilesCounte

	//fmt.Println(flightMiles)
	///	last part of the mileage per flight
	outputFlightMiles := ""
	outputPassengerMiles := ""
	for key, value := range passengerMileTracker {
		outputPassengerMiles = outputPassengerMiles + key + "-" + fmt.Sprint(value) + "\n"
	}

	for key, value := range flightMiles {
		outputFlightMiles = outputFlightMiles + key + "-" + value + "\n"
	}
	//fmt.Println(outputFlightMiles)
	//Miles of each passenger
	//Dictionary of passenger numbers that is cross referenced with the flight miles dictionary and each match is incremented with the value of flight miles
	//fmt.Println(currentProcessorDataArray)
	//outputFlightMiles = outputFlightMiles + ":" + outputFlightMiles
	outputFile("outputMilesPerFlight", strings.Replace(outputFlightMiles, "-", "        ", -1), 3, strings.Replace(outputPassengerMiles, "-", "                ", -1)) // Calls output to file function for flights from each airport task

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
