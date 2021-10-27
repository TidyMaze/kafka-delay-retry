package kafka_delay_retry

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/TidyMaze/kafka-delay-retry/test_utils"
	"go.uber.org/goleak"
)

const SIZE_PRODUCED int = 500

// testing main app
func TestApp(t *testing.T) {

	defer goleak.VerifyNone(t)

	testAppFinished := make(chan bool)

	defer func() {
		fmt.Println("Waiting for testAppFinished")
		<-testAppFinished
		close(testAppFinished)
	}()

	inputTopic := "test-app-input-topic"
	retryTopic := "^.*-retry"
	outputTopic := "test-app-output-topic"

	config := KafkaDelayRetryConfig{
		inputTopic:       retryTopic,
		bootstrapServers: "localhost:29092",
	}

	client := getAdminClient(config.bootstrapServers)

	ctx := context.Background()

	createTopics(ctx, client, []string{inputTopic, outputTopic, inputTopic + "-retry"}, 1, 1)

	clearTestDB()

	ctxRetryApp, cancelRetryApp := context.WithCancel(ctx)
	NewKafkaDelayRetryApp(ctxRetryApp, config)
	defer cancelRetryApp()

	test_utils.ProduceTestMessages(inputTopic, SIZE_PRODUCED)

	ctxTestApp, cancelTestApp := context.WithCancel(ctx)
	go StartTestApp(ctxTestApp, inputTopic, outputTopic, config.bootstrapServers, testAppFinished)
	defer cancelTestApp()

	test_utils.ExpectMessages(t, outputTopic, 5*time.Minute, SIZE_PRODUCED)
}

func clearTestDB() {
	repo := SqliteMessageRepository()
	repo.Truncate()
	db, _ := repo.db.DB()
	closeError := db.Close()

	if closeError != nil {
		panic(fmt.Sprintf("Failed to close db: %s\n", closeError))
	}
}
