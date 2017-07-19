package deliver

import (
	"time"
)

type Stat struct {
	TotalRequest     int64
	StartTime        time.Time
	RequestPerSecond int64
	LastTotalRequest int64
	LastStatTime     time.Time
}
