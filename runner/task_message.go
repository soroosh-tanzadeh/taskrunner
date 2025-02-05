package runner

type TaskMessage struct {
	ID                 string `json:"-"`
	Unique             bool   `json:"unique"`
	UniqueFor          int64  `json:"unique_for"`
	UniqueKey          string `json:"unique_key"`
	UniqueLockValue    string `json:"unique_lock_value"`
	ReservationTimeout int64  `json:"reservation_timeout"`
	TaskName           string `json:"task_name"`
	Payload            any    `json:"payload"`
}
