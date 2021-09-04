package kafka_delay_retry

import (
	"fmt"
	"time"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

type MessageRepository struct {
	db *gorm.DB
}

func SqliteMessageRepository() *MessageRepository {
	return SqliteMessageRepositoryFromFile("database.db")
}

func SqliteMessageRepositoryFromFile(file string) *MessageRepository {
	return NewMessageRepository(sqlite.Open(file))
}

func NewMessageRepository(dialector gorm.Dialector) *MessageRepository {
	db, err := gorm.Open(dialector, &gorm.Config{})
	if err != nil {
		// log err with message
		panic(fmt.Sprintf("Error opening database: %v", err))
	}

	mr := &MessageRepository{
		db: db,
	}

	mr.migrate()

	return mr
}

func (mr *MessageRepository) migrate() {
	err := mr.db.AutoMigrate(&StoredMessage{})
	if err != nil {
		panic(fmt.Sprintf("Error auto-migrating: %v", err))
	}
}

// truncate db table
func (mr *MessageRepository) Truncate() {
	// Cleanup before running app
	err := mr.db.Exec("DELETE FROM stored_messages").Error
	if err != nil {
		panic(fmt.Sprintf("Error cleaning up: %v", err))
	}
}

func (mr *MessageRepository) Create(message *StoredMessage) {
	err := mr.db.Create(message).Error
	if err != nil {
		panic(fmt.Sprintf("Error saving message: %v", err))
	}
}

func (mr *MessageRepository) FindById(id int) StoredMessage {
	var msg StoredMessage
	err := mr.db.First(&msg, id).Error
	if err != nil {
		panic(fmt.Sprintf("Error finding message: %v", err))
	}
	return msg
}

// find all stored messages who WaitUntil is less than or equal to now
func (mr *MessageRepository) FindAllExpired() []StoredMessage {
	var msgs []StoredMessage
	err := mr.db.Where("wait_until <= ?", time.Now()).Find(&msgs).Error
	if err != nil {
		panic(fmt.Sprintf("Error finding messages: %v", err))
	}
	return msgs
}

// delete a stored message
func (mr *MessageRepository) Delete(msg StoredMessage) {
	err := mr.db.Delete(&msg).Error
	if err != nil {
		panic(fmt.Sprintf("Error deleting message: %v", err))
	}
}
