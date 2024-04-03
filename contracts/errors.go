package contracts

type NoNewMessageError string

func (e NoNewMessageError) Error() string { return string(e) }

const ErrNoNewMessage = NoNewMessageError("no new message")

type MessageNotFoundError string

func (e MessageNotFoundError) Error() string { return string(e) }

const ErrMessageNotFound = MessageNotFoundError("message not found")

type NotInitializedError string

func (e NotInitializedError) Error() string { return string(e) }

const ErrNotInitialized = NotInitializedError("you should initialize rmq first")
