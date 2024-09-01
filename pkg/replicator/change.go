package replicator

import (
	"time"

	"github.com/jackc/pglogrepl"
)

type Change struct {
	Watermark
	Message
}

type Watermark struct {
	LSN        pglogrepl.LSN
	ServerTime time.Time
}

type Message struct {
}
