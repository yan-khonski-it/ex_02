package main

import "time"

// listenCancellation returns a channel that will notify when
// a cancellation signal is delivered to our application.
func listenCancellation() <-chan bool {
	canc := make(chan bool)
	time.AfterFunc(time.Second*6, func() {
		close(canc)
	})
	return canc
}
