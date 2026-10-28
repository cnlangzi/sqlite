package sqlite

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// TestBufferConfig_Validate_Defaults verifies that Validate fills every
// unset field, including the task queue capacity.
func TestBufferConfig_Validate_Defaults(t *testing.T) {
	cfg := BufferConfig{}
	cfg.Validate()

	assert.Equal(t, 100, cfg.Size)
	assert.Equal(t, 100*time.Millisecond, cfg.FlushInterval)
	assert.Equal(t, 100, cfg.QueueCapacity)
}

// TestBufferConfig_Validate_QueueCapacity verifies that a caller-supplied
// queue capacity survives validation, and that a non-positive value falls
// back to the default.
func TestBufferConfig_Validate_QueueCapacity(t *testing.T) {
	cfg := BufferConfig{Size: 10, FlushInterval: time.Second, QueueCapacity: 7}
	cfg.Validate()
	assert.Equal(t, 7, cfg.QueueCapacity)

	cfg.QueueCapacity = 0
	cfg.Validate()
	assert.Equal(t, 100, cfg.QueueCapacity)

	cfg.QueueCapacity = -1
	cfg.Validate()
	assert.Equal(t, 100, cfg.QueueCapacity)
}

// TestDefaultBufferConfig_QueueCapacityEnv verifies that the queue capacity
// can be configured through the environment, like the other buffer settings.
func TestDefaultBufferConfig_QueueCapacityEnv(t *testing.T) {
	t.Setenv("SQLITE_BUFFER_QUEUE_CAPACITY", "7")
	assert.Equal(t, 7, DefaultBufferConfig().QueueCapacity)
}

// TestDefaultBufferConfig_QueueCapacityEnvInvalid verifies that an
// unparseable or non-positive environment value falls back to the default.
func TestDefaultBufferConfig_QueueCapacityEnvInvalid(t *testing.T) {
	t.Setenv("SQLITE_BUFFER_QUEUE_CAPACITY", "not-a-number")
	assert.Equal(t, 100, DefaultBufferConfig().QueueCapacity)

	t.Setenv("SQLITE_BUFFER_QUEUE_CAPACITY", "0")
	assert.Equal(t, 100, DefaultBufferConfig().QueueCapacity)
}

// TestNewWriter_QueueCapacity verifies that the writer's task queue is sized
// from the configured capacity rather than a hardcoded value.
func TestNewWriter_QueueCapacity(t *testing.T) {
	db, cleanup := newTestDB(t)
	defer cleanup()

	bw := NewWriter(db, BufferConfig{
		Size:          10,
		FlushInterval: 100 * time.Second,
		QueueCapacity: 4,
	})
	defer bw.Close()

	assert.Equal(t, 4, cap(bw.tasks))
}
