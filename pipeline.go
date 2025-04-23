package idempipe

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

// ErrTaskSkipped indicates that a task did not execute because an upstream prerequisite failed or the context was cancelled.
var ErrTaskSkipped = errors.New("task skipped due to prerequisite failure or cancellation")

// Task represents a single unit of work in the pipeline.
// Its fields are unexported and should only be set via NewTask.
type Task struct {
	name          string
	prerequisites []*Task // Internal slice, copied during construction
	function      func(ctx context.Context) error
	retryStrategy func(err error, attempt int) error
	shouldSkip    func() bool // Optional: Function to determine if the task should be skipped
}

// TaskOptions are the options for a task.
// It is used to create a task with a custom name, prerequisites, function, retry strategy (optional),
// and skip condition (optional).
// The retry strategy is an optional function that takes an error and an attempt number, and returns an error.
// An error returned by the retry strategy indicates that the task should not be retried.
// E.g. if the task is a network call, the retry strategy might be to sleep and retry up to 3 times.
// Context cancellation is handled automatically by the pipeline, so the retry strategy does not need to handle it.
// The skip condition is an optional function that returns true if the task should be skipped.
// If skipped, the task is considered successfully completed (error is nil) for dependency purposes.
type TaskOptions struct {
	Name          string
	Prerequisites []*Task
	Function      func(ctx context.Context) error
	RetryStrategy func(err error, attempt int) error
	ShouldSkip    func() bool // Optional: If this returns true, the task is skipped.
}

// NewTask creates and validates a new Task.
// It copies the provided prerequisites to prevent external modification.
func NewTask(opts TaskOptions) (*Task, error) {
	if opts.Name == "" {
		return nil, errors.New("task name cannot be empty")
	}
	if opts.Function == nil {
		return nil, errors.New("task function cannot be nil")
	}

	// Create a copy of the prerequisites slice to prevent external modification
	prerequisitesCopy := make([]*Task, len(opts.Prerequisites))
	if opts.Prerequisites != nil {
		copy(prerequisitesCopy, opts.Prerequisites)
	}

	return &Task{
		name:          opts.Name,
		prerequisites: prerequisitesCopy, // Use the copy
		function:      opts.Function,
		retryStrategy: opts.RetryStrategy,
		shouldSkip:    opts.ShouldSkip, // Assign the skip condition
	}, nil
}

// String provides a simple string representation for the task, primarily for logging/debugging.
func (task *Task) String() string {
	return fmt.Sprintf("Task(%s)", task.name)
}

// Options configure a new Pipeline.
type Options struct {
	// Name is an optional name for the pipeline, used in logging. Defaults to "Default".
	Name string
	// Logger is an optional logging function. If nil, logs will be printed to standard output.
	Logger func(format string)
	// Tasks is the list of all tasks that are part of this pipeline. This field is required.
	// These tasks should be created using NewTask.
	Tasks []*Task
	// ConcurrencyLimit is the maximum number of tasks that can run concurrently.
	// A value of 0 or less means no limit.
	ConcurrencyLimit int
	// DefaultRetryStrategy is an optional function that can be used to customize the default retry logic for all tasks within the pipeline.
	// It should return an error if the task should not be retried.
	// If nil, the default retry strategy will be used. (no retries)
	// e.g. if the task is a network call, the retry strategy might be to sleep and retry up to 3 times.
	// Context cancellation is handled automatically by the pipeline, so the retry strategy does not need to handle it.
	DefaultRetryStrategy func(err error, attempt int) error
	// MaximumRetryStrategyDepth is the maximum number of times RetryStrategy will be allowed to be called for a given task.
	// If nil; the default maximum retries will be used. (1000 - arbitrary large number)
	// For most use cases, this should not be overridden; the retry strategy should utilize a backoff strategy, or the task function should use a polling strategy.
	// If you wish to deliberately allow infinite retries, set this to -1.
	MaximumRetryStrategyDepth *int
}

// Pipeline manages the execution of a directed acyclic graph (DAG) of tasks.
type Pipeline struct {
	name                      string
	tasks                     []*Task
	logger                    func(format string)
	concurrencyLimiter        chan struct{}
	retryStrategy             func(err error, attempt int) error
	maximumRetryStrategyDepth *int
}

// NewPipeline creates a new pipeline instance from the given options.
// It validates the options, ensuring that tasks are provided.
// It sets up default values for Name and Logger if they are not provided.
func NewPipeline(opts Options) (*Pipeline, error) {
	var pipelineName = "Default"
	if opts.Name != "" {
		pipelineName = opts.Name
	}
	if opts.Tasks == nil {
		return nil, fmt.Errorf("pipeline options must include all of the pipeline's tasks")
	}
	if opts.Logger == nil {
		opts.Logger = func(format string) {
			fmt.Print(format)
		}
	}

	var defaultRetryStrategy func(err error, attempt int) error
	if opts.DefaultRetryStrategy != nil {
		defaultRetryStrategy = opts.DefaultRetryStrategy
	} else {
		defaultRetryStrategy = func(err error, attempt int) error {
			return err
		}
	}

	var concurrencyLimiter chan struct{}
	if opts.ConcurrencyLimit > 0 {
		concurrencyLimiter = make(chan struct{}, opts.ConcurrencyLimit)
	}
	p := &Pipeline{
		name:                      pipelineName,
		tasks:                     opts.Tasks,
		logger:                    opts.Logger,
		concurrencyLimiter:        concurrencyLimiter,
		retryStrategy:             defaultRetryStrategy,
		maximumRetryStrategyDepth: opts.MaximumRetryStrategyDepth,
	}
	return p, nil
}

// log is an internal helper method for logging messages prefixed with the pipeline name and timestamp.
func (p *Pipeline) log(format string, args ...any) {
	p.logger(fmt.Sprintf("%s [Pipeline %s] %s\n", time.Now().UTC().Format("2006/01/02 15:04:05.000 MST"), p.name, fmt.Sprintf(format, args...)))
}

// Run executes the pipeline tasks respecting their prerequisites.
// It runs tasks concurrently when possible. If any task fails, subsequent tasks
// that depend on it (directly or indirectly) will be skipped.
// The execution can be cancelled via the provided context.
// It returns the first error encountered during execution, or nil if all tasks
// completed successfully or were skipped due to upstream failures.
// It also detects and reports circular prerequisites before starting execution.
func (p *Pipeline) Run(ctx context.Context) error {
	if err := detectCycles(p.tasks); err != nil {
		return err
	}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup
	doneChans := make(map[*Task]chan error, len(p.tasks))
	for _, s := range p.tasks {
		doneChans[s] = make(chan error, 1)
	}

	var taskErrorsMu sync.Mutex
	taskErrors := make(map[*Task]error, len(p.tasks))

	p.log("Starting pipeline execution...")
	for _, task := range p.tasks {
		wg.Add(1)
		go p.executeTask(runCtx, task, doneChans, &taskErrorsMu, taskErrors, &wg)
	}

	return p.processPipelineResults(ctx, cancel, &wg, taskErrors, &taskErrorsMu)
}

// processPipelineResults waits for all tasks to complete or for external cancellation,
// logs the outcome, and returns the final pipeline error.
func (p *Pipeline) processPipelineResults(
	ctx context.Context,
	cancel context.CancelFunc,
	wg *sync.WaitGroup,
	taskErrors map[*Task]error,
	taskErrorsMu *sync.Mutex,
) error {
	waitDone := make(chan struct{})
	go func() {
		wg.Wait()
		close(waitDone)
	}()

	select {
	case <-ctx.Done():
		p.log("External context cancelled. Waiting for tasks to finish...")
		cancel()
		<-waitDone
		p.log("Tasks finished after cancellation signal.")
		return ctx.Err()

	case <-waitDone:
		taskErrorsMu.Lock()
		actualErrors := make(map[*Task]error)
		for task, err := range taskErrors {
			if err != nil && !errors.Is(err, ErrTaskSkipped) {
				actualErrors[task] = err
			}
		}
		taskErrorsMu.Unlock()

		if len(actualErrors) > 0 {
			p.log("Completed with %d error(s):", len(actualErrors))
			for task, err := range actualErrors {
				p.log("  - Task %s: %v", task, err)
			}

			return fmt.Errorf("pipeline %q failed with %d error(s): %v", p.name, len(actualErrors), actualErrors)
		}

		p.log("All tasks completed successfully.")
		return nil
	}
}

// runTaskWithRetry contains the core logic for executing a task's function,
// handling context cancellation checks immediately after execution, and managing retries.
func (p *Pipeline) runTaskWithRetry(
	runCtx context.Context,
	task *Task,
	taskErrorsMu *sync.Mutex,
	taskErrors map[*Task]error,
) {
	var attempt int
	for {
		select {
		case <-runCtx.Done():
			p.recordSkipError(task, runCtx.Err(), taskErrorsMu, taskErrors)
			return
		default:
		}

		taskExecError := p.runTaskFunction(runCtx, task)

		if errors.Is(taskExecError, context.Canceled) {
			p.recordSkipError(task, taskExecError, taskErrorsMu, taskErrors)
			return
		}

		if taskExecError == nil {
			p.recordTaskResult(task, nil, taskErrorsMu, taskErrors)
			return
		}

		retryStrategy := task.retryStrategy
		if retryStrategy == nil {
			retryStrategy = p.retryStrategy
		}

		retryErr := retryStrategy(taskExecError, attempt)
		if retryErr != nil {
			p.recordTaskResult(task, retryErr, taskErrorsMu, taskErrors)
			return
		}

		if p.exceedsMaximumRetryDepth(attempt) {
			maxRetryErr := fmt.Errorf("%s maximum retry depth reached (attempt %d), final error: %w", task, attempt, taskExecError)
			p.recordTaskResult(task, maxRetryErr, taskErrorsMu, taskErrors)
			return
		}

		attempt++
		p.log("%s: Retrying (attempt %d) after error: %v", task, attempt, taskExecError)
	}
}

// executeTask handles the setup for a single task's execution, including prerequisite waiting,
// context checks before execution, concurrency limiting, and finally calls runTaskWithRetry.
func (p *Pipeline) executeTask(
	runCtx context.Context,
	task *Task,
	doneChans map[*Task]chan error,
	taskErrorsMu *sync.Mutex,
	taskErrors map[*Task]error,
	wg *sync.WaitGroup,
) {
	defer wg.Done()
	defer close(doneChans[task])

	// Check skip condition first
	if task.shouldSkip != nil && task.shouldSkip() {
		p.log("%s: Skipping due to skip condition.", task)
		taskErrorsMu.Lock()
		taskErrors[task] = nil
		taskErrorsMu.Unlock()
		return
	}

	prerequisiteSkipError := p.waitForPrerequisites(runCtx, task, doneChans, taskErrorsMu, taskErrors)
	if prerequisiteSkipError != nil {
		p.recordSkipError(task, prerequisiteSkipError, taskErrorsMu, taskErrors)
		return
	}

	if err := checkContextCancellation(runCtx); err != nil {
		p.recordSkipError(task, err, taskErrorsMu, taskErrors)
		return
	}

	if p.concurrencyLimiter != nil {
		select {
		case p.concurrencyLimiter <- struct{}{}:
			defer func() { <-p.concurrencyLimiter }()
		case <-runCtx.Done():
			p.recordSkipError(task, runCtx.Err(), taskErrorsMu, taskErrors)
			return
		}
	}

	p.runTaskWithRetry(runCtx, task, taskErrorsMu, taskErrors)
}

// waitForPrerequisites waits for all prerequisites of a task to complete.
// It returns an error if any prerequisite fails or if the context is cancelled.
func (p *Pipeline) waitForPrerequisites(
	runCtx context.Context,
	task *Task,
	doneChans map[*Task]chan error,
	taskErrorsMu *sync.Mutex,
	taskErrors map[*Task]error,
) error {
	var prerequisiteSkipError error
	var cancelledWhileWaiting bool

	for _, prerequisite := range task.prerequisites {
		p.log("%s: Waiting for prerequisite %s...", task, prerequisite)
		select {
		case <-doneChans[prerequisite]:
			taskErrorsMu.Lock()
			prerequisiteErr := taskErrors[prerequisite]
			taskErrorsMu.Unlock()
			if prerequisiteErr != nil {
				if prerequisiteSkipError == nil {
					prerequisiteSkipError = prerequisiteErr
				}
				p.log("%s: Prerequisite %s did not complete successfully (%v).", task, prerequisite, prerequisiteErr)
			} else {
				p.log("%s: Prerequisite %s completed successfully.", task, prerequisite)
			}
		case <-runCtx.Done():
			p.log("%s: Cancelled by context while waiting for prerequisite %s.", task, prerequisite)
			if prerequisiteSkipError == nil {
				prerequisiteSkipError = runCtx.Err()
			}
			cancelledWhileWaiting = true
		}
		if cancelledWhileWaiting {
			break
		}
	}
	return prerequisiteSkipError
}

// defaultMaximumRetryDepth is the default maximum number of retries to allow for a given task, preventing infinite retries.
// If undefined in Options; the default maximum retries will be used. (1000 - arbitrary large number)
var defaultMaximumRetryDepth = 1000

// exceedsMaximumRetryDepth checks if the maximum retry depth has been exceeded.
// If it has, it records the error and returns it.
func (p *Pipeline) exceedsMaximumRetryDepth(attempt int) bool {
	if p.maximumRetryStrategyDepth == nil {
		return attempt >= defaultMaximumRetryDepth
	}

	if *p.maximumRetryStrategyDepth == -1 {
		return false
	}

	return attempt >= *p.maximumRetryStrategyDepth
}

// recordSkipError logs and records a skip error for a task.
func (p *Pipeline) recordSkipError(
	task *Task,
	skipReason error,
	taskErrorsMu *sync.Mutex,
	taskErrors map[*Task]error,
) {
	skipErr := fmt.Errorf("%w: %v", ErrTaskSkipped, skipReason)
	p.log("%s: Skipping: %v", task, skipErr)
	taskErrorsMu.Lock()
	if taskErrors[task] == nil {
		taskErrors[task] = skipErr
	}
	taskErrorsMu.Unlock()
}

// checkContextCancellation checks if the context has been cancelled before starting the task.
func checkContextCancellation(runCtx context.Context) error {
	select {
	case <-runCtx.Done():
		return fmt.Errorf("%w: context cancelled", ErrTaskSkipped)
	default:
		return nil
	}
}

// runTaskFunction executes the task's function and handles panics.
// It returns the error from the function or a panic error.
func (p *Pipeline) runTaskFunction(runCtx context.Context, task *Task) (taskExecError error) {
	p.log("%s: Starting...", task)
	defer func() {
		if r := recover(); r != nil {
			taskExecError = fmt.Errorf("%s panicked: %v", task, r)
			p.log("%s: PANICKED: %v", task, r)
		}
	}()
	taskExecError = task.function(runCtx)
	return taskExecError
}

// recordTaskResult logs the task outcome and updates the shared error state.
func (p *Pipeline) recordTaskResult(
	task *Task,
	taskExecError error,
	taskErrorsMu *sync.Mutex,
	taskErrors map[*Task]error,
) {
	taskErrorsMu.Lock()
	if taskExecError != nil {
		p.log("%s: Failed: %v", task, taskExecError)
		taskErrors[task] = taskExecError
	} else {
		p.log("%s: Completed", task)
		taskErrors[task] = nil
	}
	taskErrorsMu.Unlock()
}

// visitState represents the state of a node during cycle detection DFS.
type visitState int

const (
	// unvisited indicates a node has not been visited yet.
	unvisited visitState = iota
	// visiting indicates a node is currently being visited (in the current recursion stack).
	visiting
	// visited indicates a node and all its descendants have been fully visited.
	visited
)

// detectCycles checks for circular prerequisites among the tasks using Depth First Search.
// It returns an error describing the cycle if one is found, otherwise nil.
// It also checks if all declared prerequisites exist within the provided tasks list.
func detectCycles(tasks []*Task) error {
	state := make(map[*Task]visitState)
	for _, task := range tasks {
		state[task] = unvisited
	}

	for _, task := range tasks {
		if state[task] == unvisited {
			var path []*Task
			if err := dfsVisit(task, state, path); err != nil {
				return err
			}
		}
	}
	return nil
}

// dfsVisit is a helper function for detectCycles that performs the DFS traversal.
// It updates the state of visited nodes and detects cycles.
// path keeps track of the nodes in the current recursion stack to identify the cycle path.
func dfsVisit(task *Task, state map[*Task]visitState, path []*Task) error {
	state[task] = visiting
	path = append(path, task)

	for _, prerequisite := range task.prerequisites {
		if prerequisite == nil {
			return fmt.Errorf("task %q has a nil prerequisite entry", task.name)
		}

		if _, ok := state[prerequisite]; !ok {
			return fmt.Errorf("prerequisite %q for task %q not found in the defined tasks", prerequisite.name, task.name)
		}

		switch state[prerequisite] {
		case visiting:
			cyclePathStr := buildCyclePathString(path, prerequisite)
			return fmt.Errorf("circular prerequisite detected: %s", cyclePathStr)
		case unvisited:
			if err := dfsVisit(prerequisite, state, path); err != nil {
				return err
			}
		case visited:
		}
	}

	state[task] = visited
	return nil
}

// buildCyclePathString constructs a string representation of the detected cycle path.
// It sorts the nodes in the cycle alphabetically for consistent output.
func buildCyclePathString(path []*Task, cycleStartNode *Task) string {
	startIndex := -1
	for i, node := range path {
		if node == cycleStartNode {
			startIndex = i
			break
		}
	}
	if startIndex == -1 {
		return "[internal error building cycle path: start node not found in path]"
	}
	cycleNodes := path[startIndex:]
	sort.Slice(cycleNodes, func(i, j int) bool {
		return cycleNodes[i].name < cycleNodes[j].name
	})
	var parts []string
	for _, node := range cycleNodes {
		parts = append(parts, node.name)
	}
	cyclePathStr := strings.Join(parts, " -> ") + " -> " + cycleStartNode.name
	return cyclePathStr
}
