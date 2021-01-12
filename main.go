package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"runtime"
)

func main() {

	noCPUS := runtime.NumCPU()
	fmt.Println(noCPUS)
	counter := 0

	file, err := os.Open("./data/test/AComp_Passenger_data_no_error.csv")

	if err != nil {
		log.Fatal(err)
	}

	defer file.Close()

	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		counter++
		//fmt.Println(scanner.Text())
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

	fmt.Println("Number of lines is: ", counter)
	fmt.Println("Lines/Processors: ", counter/noCPUS)
	fmt.Println("Lines/Processors: ", counter%noCPUS == 0)

	currentCounter := counter

	if currentCounter%noCPUS != 0 {
		for {
			currentCounter++
			if currentCounter%noCPUS == 0 {
				fmt.Println(currentCounter)
				fmt.Println("Lines/Processors: ", currentCounter/noCPUS)
				break
			}
		}

	}

}
