package runner

type TaskRunnerError string

func (tre TaskRunnerError) Error() string {
	return string(tre)
}

type TaskExecutionError struct {
	taskname string
	err      error
}

func NewTaskExecutionError(task string, err error) TaskExecutionError {
	return TaskExecutionError{
		err:      err,
		taskname: task,
	}
}

func (t TaskExecutionError) Error() string {
	return t.taskname + ":" + t.err.Error()
}

func (t TaskExecutionError) GetError() error {
	return t.err
}

func (t TaskExecutionError) GetTaskName() string {
	return t.taskname
}
