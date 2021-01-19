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
	numberofCPUs := runtime.NumCPU() //Calculates Current CPUs
	//Prints Processors Available

	processorOutputOne := make(map[string]int)
	processorOutputTwo := make(map[string]int)
	processorOutputThr := make(map[string]int)
	processorOutputFou := make(map[string]int)
	processorOutputFiv := make(map[string]int)
	processorOutputSix := make(map[string]int)
	processorOutputSev := make(map[string]int)
	processorOutputEig := make(map[string]int)

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
	file, err = os.Create("./output/" + "test1" + ".txt")
	if err != nil {
		return
	}
	defer file.Close()
	testString := ""
	for i := 0; i < len(processorAllocatedLines[2]); i++ {
		testString = testString + processorAllocatedLines[2][i][0] + "," + processorAllocatedLines[2][i][1] + "," + processorAllocatedLines[2][i][2] + "," + processorAllocatedLines[2][i][3] + "," + processorAllocatedLines[2][i][4] + "," + processorAllocatedLines[2][i][5] + "\n"
	}
	file.WriteString("Entry 0 below: " + "\n\n" + testString)

	//Closes file
	//fmt.Println(processorAllocatedLines[0][0][1])
	//fmt.Println(processorAllocatedLines[0])
	outputData(unknownData, "unknownDataEntries", 0)
	//Processor One
	//counter := make(map[string]int)
	//c2 := make(chan string)
	//c3 := make(chan string)
	//c4 := make(chan string)
	//c5 := make(chan string)
	//c6 := make(chan string)
	//c7 := make(chan string)
	//c8 := make(chan string)
	switch numberofCPUs {
	case 0:
		fmt.Println("Insuffiencient number of processors - Program exiting")
		break
	case 1:
		go mapper(processorAllocatedLines[0], airportData, processorOutputOne)
	case 2:
		go mapper(processorAllocatedLines[0], airportData, processorOutputOne)
		go mapper(processorAllocatedLines[1], airportData, processorOutputTwo)
	case 3:
		go mapper(processorAllocatedLines[0], airportData, processorOutputOne)
		go mapper(processorAllocatedLines[1], airportData, processorOutputTwo)
		go mapper(processorAllocatedLines[2], airportData, processorOutputThr)
	case 4:
		go mapper(processorAllocatedLines[0], airportData, processorOutputOne)
		go mapper(processorAllocatedLines[1], airportData, processorOutputTwo)
		go mapper(processorAllocatedLines[2], airportData, processorOutputThr)
		go mapper(processorAllocatedLines[3], airportData, processorOutputFou)
	case 5:
		go mapper(processorAllocatedLines[0], airportData, processorOutputOne)
		go mapper(processorAllocatedLines[1], airportData, processorOutputTwo)
		go mapper(processorAllocatedLines[2], airportData, processorOutputThr)
		go mapper(processorAllocatedLines[3], airportData, processorOutputFou)
		go mapper(processorAllocatedLines[4], airportData, processorOutputFiv)
	case 6:
		go mapper(processorAllocatedLines[0], airportData, processorOutputOne)
		go mapper(processorAllocatedLines[1], airportData, processorOutputTwo)
		go mapper(processorAllocatedLines[2], airportData, processorOutputThr)
		go mapper(processorAllocatedLines[3], airportData, processorOutputFou)
		go mapper(processorAllocatedLines[4], airportData, processorOutputFiv)
		go mapper(processorAllocatedLines[5], airportData, processorOutputSix)
	case 7:
		go mapper(processorAllocatedLines[0], airportData, processorOutputOne)
		go mapper(processorAllocatedLines[1], airportData, processorOutputTwo)
		go mapper(processorAllocatedLines[2], airportData, processorOutputThr)
		go mapper(processorAllocatedLines[3], airportData, processorOutputFou)
		go mapper(processorAllocatedLines[4], airportData, processorOutputFiv)
		go mapper(processorAllocatedLines[5], airportData, processorOutputSix)
		go mapper(processorAllocatedLines[6], airportData, processorOutputSev)
	case 8:
		go mapper(processorAllocatedLines[0], airportData, processorOutputOne)
		go mapper(processorAllocatedLines[1], airportData, processorOutputTwo)
		go mapper(processorAllocatedLines[2], airportData, processorOutputThr)
		go mapper(processorAllocatedLines[3], airportData, processorOutputFou)
		go mapper(processorAllocatedLines[4], airportData, processorOutputFiv)
		go mapper(processorAllocatedLines[5], airportData, processorOutputSix)
		go mapper(processorAllocatedLines[6], airportData, processorOutputSev)
		go mapper(processorAllocatedLines[7], airportData, processorOutputEig)
		time.Sleep(1 * time.Second)
		reducer(processorOutputOne, processorOutputTwo, processorOutputThr, processorOutputFou, processorOutputFiv, processorOutputSix, processorOutputSev, processorOutputEig)
	}

	//fmt.Println(counter)
	//testString := "TEST"
	//go shuffleInput := printlineGoTest(testString)
	//time.Sleep(1 * time.Second)
	//fmt.Println(processorOutputOne)
}

//func printlineGoTest(text string) {
//	fmt.Println(text)
//}
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
