package main

import (
	"fmt"
	"time"
)

func main() {
	items := 20
	publishers := 5

	// upload ids
	msgs := make(chan int, items)

	// restrcit # of publishers
	semaphores := make(chan int, publishers)
	for i := 1; i <= publishers; i++ {
		semaphores <- 1
	}

	for i := 1; i <= items; i++ {
		// 20 Uploaders
		go func(iter int) {
			time.Sleep(time.Second * 2)
			msgs <- iter
			fmt.Println("Uploaded ", iter)
		}(i)

		// 5 Publishers
		go func() {
			<-semaphores
			fmt.Println("Grab semaphore")
			uploadid, more := <-msgs
			if more {
				time.Sleep(time.Second * 2)
				fmt.Println("Published ", uploadid)
			}
			semaphores <- 1 // put back the semaphore
			fmt.Println("Release semaphore")
		}()
	}
	var num int
	fmt.Scanf("Enter a number %d", &num)
}
