package runner

type TaskRunnerError string

func (tre TaskRunnerError) Error() string {
	return string(tre)
}
