package runner

type DelayedTask struct {
	Id      string `json:"id"`
	Task    string `json:"task"`
	Time    int64  `json:"time"`
	Payload any    `json:"payload"`
}
