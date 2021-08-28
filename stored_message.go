package kafka_delay_retry

import (
	"gorm.io/gorm"
)

type StoredMessage struct {
	gorm.Model
	name string
}
