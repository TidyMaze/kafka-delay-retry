package internal

import (
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"gorm.io/gorm"
)

type StoredMessage struct {
	gorm.Model
	Topic        string
	Partition    int32
	Offset       kafka.Offset
	Key          string
	Value        string
	WaitUntil    time.Time
	WaitDuration time.Duration
}
