package contracts

import "context"

type HeartBeatFunc func(ctx context.Context) error

type StreamConsumeFunc func(context.Context, Message, HeartBeatFunc) error
