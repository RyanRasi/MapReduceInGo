# MapReduceInGo
An implementation mimicking the abilities of mapReduce in Go. Uses concurrency and error detection of input files.

Run using...
```
go run main.go
```

This implementation utilises the two text files. One being a passenger manifest list and the other for the list of 30 airports.
Five output text files are compiled from this.
1. The number of flights from each airport and a list of any airports not used.
2. A list of flights based on the Flight id, including the passenger Id, relevant IATA/FAA codes, the departure time, the arrival time in HH:MM:SS format, and the flight times.
3. The number of passengers on each flight.
4. The line-of-sight (nautical) miles for each flight and the total travelled by each passenger and the output the passenger having earned the highest air miles.
5. Any error cases in the passenger data file.

Each line in the passenger data file is in the form of...

Passenger ID, Flight ID, From airport IATA/FAA code, Destination airport IATA/FAA code, Departure time (GMT), Total flight time (mins)

And is formatted in the style of...

XXXnnnnXXn, XXXnnnnX, XXX, XXX, n[10 - in UNIX Epoch time, n[1..4]

Where X is Uppercase ASCII, n is digit 0..9 and [n..m] is the min/max range of the number
of digits/characters in a string.

For example:

UES9151GS5,SQU6245R,DEN,FRA,1420564460,1049

The errors were thus discovered using a REGEX statement of...
```
^[a-zA-Z]{3}[0-9]{4}[a-zA-Z]{2}[0-9][,][a-zA-Z]{3}[0-9]{4}[a-zA-Z][,][a-zA-Z]{3}[,][a-zA-Z]{3},[0-9]{10}[,][0-9]{1,4}
```

This was looped through all of the lines using the buffered reader where they are then sent to text file 5 for the error cases.

A 3D array is created to hold the correct lines for each processor available.
For example the arrays structure is [currentCPU][currentLine][0-5]

If there are 8 processors available, and 400 correct lines in the file then the array size will be...

[8][50][5]

The 5 at the end will always be 5 due to thats the size of each line after it is split by a comma.

4 channels are then made for each text file and for however many processors there are avaialable, then the same amount of concurrent functions are ran.
The mappers first run which sorts all of the text files per task and returns the answer via a mapDictionary to the channels. That input is then sent to the reducer to combine the answers from each channel together and output the final text file.
The code which controls the concurrent functions are as follows...
```go
	task1Channel := make(chan map[string]int)      //Passengers on each flight
	task2Channel := make(chan map[string]int)      //Flights from each airport
	task3aChannel := make(chan map[string]string)  //Total Nautical Miles - Per Flight
	task3bChannel := make(chan map[string]float64) //Total Nautical Miles - Per Passenger
	task4Channel := make(chan map[string]string)   //Flights based on the ID number

	// SENDS DATA TO THE MAPPER
	fmt.Println("MapReduce - Map and Reduce Task Started")
	for i := 0; i < numberofCPUs; i++ { //For every CPU do...
		//Opens concurrent processor and calls mapper with that processors specific input
		go mapper(processorAllocatedLines[i], airportData, task1Channel, task2Channel, task3aChannel, task3bChannel, task4Channel)

		//Sets channel return value to the dictionary map
		passengersOnEachFlightMapArray[i] = <-task1Channel
		flightsFromEachAirportMapArray[i] = <-task2Channel
		totalNauticalMilesPerFlightArray[i] = <-task3aChannel
		totalNauticalMilesPerPassengerArray[i] = <-task3bChannel
		flightsBasedOnID[i] = <-task4Channel

		//SENDS THE DATA TO THE REDUCER
		//Opens concurrent processor and calls reducer
		go reducer(passengersOnEachFlightMapArray, flightsFromEachAirportMapArray, totalNauticalMilesPerFlightArray, totalNauticalMilesPerPassengerArray, flightsBasedOnID, numberofCPUs)
	}
```
The mapper functions for each text file consist of simple algorithms of counting flights, conversions and cross referencing with the airport list text file.
The reducer functions consist of combining the resultant data, removing potential duplicates and formatting the output text for the text files.
A final function also alerts the user to how long the task took.

I learned a plethora about cloud computing and mass data sorting and algorithms whilst undertaking this task and look forward to learning more in the future.
