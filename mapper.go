//Map Reduce In Go
//Student ID
//28842293

package main

func mapper(processorInput [][]string, airportData [30][4]string, counter map[string]int) {
	//Passengers on Each Flight
	//counter := make(map[string]int)
	//var data [len(processorInput)]string
	data := make([]string, len(processorInput))
	for i := 0; i < len(processorInput); i++ {
		data[i] = processorInput[i][1] + "-" + processorInput[i][2] + "-" + processorInput[i][3]
	}
	//dataArray := strings.Split(data, ",")
	for i := 0; i < len(data); i++ {
		if data[i] == "" {
			break
		}
		counter[data[i]]++
	}
	//c1 <- "counter"
}
