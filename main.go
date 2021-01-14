//Map Reduce In Go
//Student ID
//28842293

package main

import (
	"fmt"
	"runtime"
)

func main() {

	noCPUS := runtime.NumCPU() //Calculates Current CPUs
	baseLines := 0
	additionalLinesNeeded := 0
	currentLineSelected := 0
	placeholderText := "EMPTY,EMPTY,EMPTY,EMPTY,EMPTY,EMPTY"
	passengerDataPath := "./data/real/AComp_Passenger_data.csv"
	airportDataPath := "./data/real/Top30_airports_LatLong.csv"

	fmt.Println("Processors Available: ", noCPUS)

}

func inputFile(fileID string) {
}
