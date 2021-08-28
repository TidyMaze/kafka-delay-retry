package kafka_delay_retry

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"gorm.io/gorm"
)

type StoredMessage struct {
	gorm.Model
	Message kafka.Message
}
