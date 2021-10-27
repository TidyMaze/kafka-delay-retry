package kafka_delay_retry

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/TidyMaze/kafka-delay-retry/internal"
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

	config := internal.KafkaDelayRetryConfig{
		InputTopic:       retryTopic,
		BootstrapServers: "localhost:29092",
	}

	ctx := context.Background()

	client := internal.getAdminClient(config.BootstrapServers)
	internal.createTopics(ctx, client, []string{inputTopic, outputTopic, inputTopic + "-retry"}, 1, 1)

	clearTestDB()

	ctxRetryApp, cancelRetryApp := context.WithCancel(ctx)
	internal.NewKafkaDelayRetryApp(ctxRetryApp, config)
	defer cancelRetryApp()

	test_utils.ProduceTestMessages(inputTopic, SIZE_PRODUCED)

	ctxTestApp, cancelTestApp := context.WithCancel(ctx)
	go StartTestApp(ctxTestApp, inputTopic, outputTopic, config.BootstrapServers, testAppFinished)
	defer cancelTestApp()

	test_utils.ExpectMessages(t, outputTopic, 5*time.Minute, SIZE_PRODUCED)
}

func clearTestDB() {
	repo := internal.SqliteMessageRepository()
	repo.Truncate()
	db, _ := repo.Db.DB()
	closeError := db.Close()

	if closeError != nil {
		panic(fmt.Sprintf("Failed to close db: %s\n", closeError))
	}
}
