package kafka_delay_retry

import (
	"fmt"
	"testing"
	"time"
)

// testing main app
func TestApp(t *testing.T) {
	inputTopic := "test"

	app := KafkaDelayRetryApp{
		inputTopic: inputTopic,
	}

	app.start()

	produceTestMessages(inputTopic)

	// show why we are sleeping
	fmt.Println("Sleeping for 10 seconds")
	time.Sleep(10 * time.Second)

	app.stop()

	// show end of program
	fmt.Println("End of kafka test")
}
