package runner

import "time"

type PeriodicTask struct {
	Id      string        `json:"id"`
	Task    string        `json:"task"`
	Every   time.Duration `json:"duration"`
	Payload any           `json:"payload"`
}
