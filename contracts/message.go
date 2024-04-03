package contracts

type Message struct {
	queue        string
	id           string
	payload      string
	receiveCount int64
}

func NewMessage(id, queue, payload string, receiveCount int64) Message {
	return Message{
		id:           id,
		payload:      payload,
		queue:        queue,
		receiveCount: receiveCount,
	}
}

func (m Message) GetQueue() string {
	return m.queue
}

func (m Message) GetId() string {
	return m.id
}

func (m Message) GetPayload() string {
	return m.payload
}

func (m Message) GetReceiveCount() int64 {
	return m.receiveCount
}
