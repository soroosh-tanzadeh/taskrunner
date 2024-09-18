package leadership

import (
	"context"
	"errors"

	"github.com/redis/go-redis/v9"
)

type faultyScripter struct {
	breakFlag bool
	client    *redis.Client
}

func NewRedisScripter(breakFlag bool, client *redis.Client) *faultyScripter {
	return &faultyScripter{
		breakFlag: breakFlag,
		client:    client,
	}
}

func (f *faultyScripter) checkBreak() error {
	if f.breakFlag {
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
