package kafka_delay_retry

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/TidyMaze/kafka-delay-retry/test_utils"
	"github.com/confluentinc/confluent-kafka-go/kafka"
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

	res, err := client.CreateTopics(ctx, []kafka.TopicSpecification{
		{Topic: inputTopic, NumPartitions: 1, ReplicationFactor: 1},
		{Topic: outputTopic, NumPartitions: 1, ReplicationFactor: 1},
		{Topic: inputTopic + "-retry", NumPartitions: 1, ReplicationFactor: 1},
	})

	if err != nil {
		panic(fmt.Sprintf("Failed to create topics: %s\n", err))
	}

	for _, v := range res {
		if v.Error.Code() != kafka.ErrNoError && v.Error.Code() != kafka.ErrTopicAlreadyExists {
			panic(fmt.Sprintf("Failed to create topic %s: %d %s\n", v.Topic, v.Error.Code(), v.Error))
		}
	}

	app := NewKafkaDelayRetryApp(config)

	app.messageRepository.Truncate()

	test_utils.ProduceTestMessages(inputTopic, SIZE_PRODUCED)

	ctx2, cancelFn := context.WithCancel(ctx)

	defer cancelFn()

	go StartTestApp(ctx2, inputTopic, outputTopic, config.bootstrapServers, testAppFinished)

	app.start()

	defer app.stop()

	test_utils.ExpectMessages(t, outputTopic, 5*time.Minute, SIZE_PRODUCED)
}

func getAdminClient(bootstrapServers string) *kafka.AdminClient {
	conf := kafka.ConfigMap{"bootstrap.servers": bootstrapServers}

	client, err := kafka.NewAdminClient(&conf)

	if err != nil {
		panic(fmt.Sprintf("Failed to create client: %v\n", err))
	}

	return client
}
