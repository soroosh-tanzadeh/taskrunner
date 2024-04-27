package contracts

type Message struct {
	ID           string
	Payload      string
	receiveCount int64
}

func NewMessage(id, payload string, receiveCount int64) Message {
	return Message{
		ID:           id,
		Payload:      payload,
		receiveCount: receiveCount,
	}
}

func (m Message) GetId() string {
	return m.ID
}

func (m Message) GetPayload() string {
	return m.Payload
}

func (m Message) GetReceiveCount() int64 {
	return m.receiveCount
}
