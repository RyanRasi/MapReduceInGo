//Map Reduce In Go
//Student ID
//28842293

package main

import "os"

func outputData(outputText string, fileID string, outputID int) {
	//Write to txt file, flights from each airport
	file, err := os.Create("./output/" + fileID + ".txt")
	if err != nil {
		return
	}
	defer file.Close()
	if outputID == 0 {
		file.WriteString("Passenger Data Entries with Errors Below: " + "\n\n" + outputText)
	} else if outputID == 1 {
		file.WriteString("Flight Number:  Depart:    Arrival:   Number of Passengers: \n" + outputText)
	} else if outputID == 2 {
		file.WriteString("IATA/FAA Code:    Airport:                 Flights: \n" + outputText)
	} else if outputID == 3 {
		file.WriteString("Flight Number:  " + "Depart:    " + "Arrival:   " + "Nautical Miles: " + "\n" + outputText + "\n" + "Passengers sorted by miles accrued: \n\n" + "Passenger Number:         Total Miles Flown:\n")
	}
}
