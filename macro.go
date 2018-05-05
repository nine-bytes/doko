package doko

import "time"

const (
	B  = 1 << (10 * iota)
	KB
	MB
	GB
	TB
)

const (
	protocolVersion  = "0.1.0"
	pxyPoolSize      = 10
	haltDuration     = 3 * time.Second
	dialTimeout      = 5 * time.Second
	rwTimeout        = 10 * time.Second
	pxyStaleDuration = 60 * time.Second
	pingInterval     = 2 * time.Minute
	bytesForRate     = 500 * KB
)
