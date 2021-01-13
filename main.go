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
	fmt.Println(processorAllocatorArray[7])
	//End of Block function

}
