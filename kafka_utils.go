package kafka_delay_retry

import (
	"context"
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func createTopics(ctx context.Context, client *kafka.AdminClient, topics []string, numPartitions int, replicationFactor int) error {
	topicSpecifications := make([]kafka.TopicSpecification, len(topics))

	for i, topic := range topics {
		topicSpecifications[i] = kafka.TopicSpecification{
			Topic:             topic,
			NumPartitions:     numPartitions,
			ReplicationFactor: replicationFactor,
		}
	}

	res, err := client.CreateTopics(ctx, topicSpecifications)

	if err != nil {
		panic(fmt.Sprintf("Failed to create topics: %s\n", err))
	}

	for _, v := range res {
		if v.Error.Code() != kafka.ErrNoError && v.Error.Code() != kafka.ErrTopicAlreadyExists {
			return fmt.Errorf("failed to create topic %s: %d %s", v.Topic, v.Error.Code(), v.Error)
		}
	}

	return nil
}
