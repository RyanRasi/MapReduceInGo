//Map Reduce In Go
//Student ID
//28842293

package main

func mapper(processorInput [][]string, airportData [30][4]string, task1Channel chan map[string]int, task2Channel chan map[string]int) {
	//PASSENGERS ON EACH FLIGHT
	counter := make(map[string]int)             //Counter map which keeps track of the flights
	data := make([]string, len(processorInput)) //Makes an array for however big the data is
	for i := 0; i < len(processorInput); i++ {
		data[i] = processorInput[i][1] + "-" + processorInput[i][2] + "-" + processorInput[i][3] //Concatonates the flight ID and departure and arrival airports
	}
	for i := 0; i < len(data); i++ { //For however long the data is...
		if data[i] == "" { //If there is no data, then skip
			break
		}
		counter[data[i]]++ //Increment the occurances of each flight
	}
	//return counter
	task1Channel <- counter //Return the result to the main function so that the reducer can be called

	//NUMBER OF FLIGHTS FROM EACH AIRPORT

}
