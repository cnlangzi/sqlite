package sqlite

import (
	"time"
)

// Buffer holds batch writer configuration.
type Buffer struct {
	// BufferSize is the number of statements to buffer before flushing.
	// Default: 100
	BufferSize int

	// FlushInterval is the maximum time to wait before flushing.
	// Default: 100ms
	FlushInterval time.Duration
}

// Validate sets defaults and validates the configuration.
func (c *Buffer) Validate() {
	if c.BufferSize <= 0 {
		c.BufferSize = 100
	}

	if c.FlushInterval <= 0 {
		c.FlushInterval = 500 * time.Millisecond
	}
}
