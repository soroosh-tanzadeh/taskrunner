package election

import (
	"context"
	"errors"
	"sync/atomic"

	"github.com/redis/go-redis/v9"
)

type faultyScripter struct {
	breakFlag *atomic.Bool
	client    *redis.Client
}

func NewFaultyScripter(breakFlag bool, client *redis.Client) *faultyScripter {
	flag := &atomic.Bool{}
	flag.Store(breakFlag)
	return &faultyScripter{
		breakFlag: flag,
		client:    client,
	}
}

func (f *faultyScripter) checkBreak() error {
	if f.breakFlag.Load() {
		return errors.New("operation aborted: break flag is set")
	}
	return nil
}

func (f *faultyScripter) Eval(ctx context.Context, script string, keys []string, args ...interface{}) *redis.Cmd {
	if err := f.checkBreak(); err != nil {
		cmd := redis.NewCmd(ctx)
		cmd.SetErr(err)
		return cmd
	}
	return f.client.Eval(ctx, script, keys, args...)
}

func (f *faultyScripter) EvalSha(ctx context.Context, sha1 string, keys []string, args ...interface{}) *redis.Cmd {
	if err := f.checkBreak(); err != nil {
		cmd := redis.NewCmd(ctx)
		cmd.SetErr(err)
		return cmd
	}
	return f.client.EvalSha(ctx, sha1, keys, args...)
}

func (f *faultyScripter) EvalRO(ctx context.Context, script string, keys []string, args ...interface{}) *redis.Cmd {
	if err := f.checkBreak(); err != nil {
		cmd := redis.NewCmd(ctx)
		cmd.SetErr(err)
		return cmd
	}
	return f.client.EvalRO(ctx, script, keys, args...)
}

func (f *faultyScripter) EvalShaRO(ctx context.Context, sha1 string, keys []string, args ...interface{}) *redis.Cmd {
	if err := f.checkBreak(); err != nil {
		cmd := redis.NewCmd(ctx)
		cmd.SetErr(err)
		return cmd
	}
	return f.client.EvalShaRO(ctx, sha1, keys, args...)
}

func (f *faultyScripter) ScriptExists(ctx context.Context, hashes ...string) *redis.BoolSliceCmd {
	if err := f.checkBreak(); err != nil {
		cmd := redis.NewBoolSliceCmd(ctx)
		cmd.SetErr(err)
		return cmd
	}
	return f.client.ScriptExists(ctx, hashes...)
}

func (f *faultyScripter) ScriptLoad(ctx context.Context, script string) *redis.StringCmd {
	if err := f.checkBreak(); err != nil {
		cmd := redis.NewStringCmd(ctx)
		cmd.SetErr(err)
		return cmd
	}
	return f.client.ScriptLoad(ctx, script)
}
