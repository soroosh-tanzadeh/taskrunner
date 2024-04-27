package contracts

type QueueError string

const ErrNoNewMessage QueueError = "no new message"
const MessageNotFoundError QueueError = "message not found"
const NotInitializedError QueueError = "you should initialize rmq first"

func (e QueueError) Error() string { return string(e) }
