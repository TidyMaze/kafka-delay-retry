package internal

import (
	"testing"
	"time"

	"github.com/TidyMaze/kafka-delay-retry/internal"
	"github.com/stretchr/testify/assert"
)

func TestIncrease(t *testing.T) {
	assert.Equal(t, internal.IncreaseRetryDuration(time.Duration(1)*time.Second), time.Duration(2)*time.Second)
	assert.Equal(t, internal.IncreaseRetryDuration(time.Duration(20)*time.Hour), time.Duration(24)*time.Hour)
}
