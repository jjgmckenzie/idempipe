package idempipe

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// successfulTask creates a task that completes successfully after a delay.
func successfulTask(t *testing.T, name string, duration time.Duration, prerequisites ...*Task) *Task {
	t.Helper()
	task, err := NewTask(TaskOptions{
		Name:          name,
		Prerequisites: prerequisites,
		Function: func(ctx context.Context) error {
			select {
			case <-time.After(duration):
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		},
	})
	require.NoError(t, err, "NewTask failed for successful task %s", name)
	return task
}

// failingTask creates a task that fails immediately.
func failingTask(t *testing.T, name string, errMsg string, prerequisites ...*Task) *Task {
	t.Helper()
	task, err := NewTask(TaskOptions{
		Name:          name,
		Prerequisites: prerequisites,
		Function: func(ctx context.Context) error {
			return errors.New(errMsg)
		},
	})
	require.NoError(t, err, "NewTask failed for failing task %s", name)
	return task
}

// panicTask creates a task that panics.
func panicTask(t *testing.T, name string, panicMsg string, prerequisites ...*Task) *Task {
	t.Helper()
	task, err := NewTask(TaskOptions{
		Name:          name,
		Prerequisites: prerequisites,
		Function: func(ctx context.Context) error {
			panic(panicMsg)
		},
	})
	require.NoError(t, err, "NewTask failed for panic task %s", name)
	return task
}

type testLogger struct {
	mu   sync.Mutex
	logs []string
}

func (tl *testLogger) Log(format string) {
	tl.mu.Lock()
	defer tl.mu.Unlock()
	tl.logs = append(tl.logs, format)
}

func (tl *testLogger) String() string {
	tl.mu.Lock()
	defer tl.mu.Unlock()
	return strings.Join(tl.logs, "")
}

func (tl *testLogger) Contains(substr string) bool {
	tl.mu.Lock()
	defer tl.mu.Unlock()
	for _, log := range tl.logs {
		if strings.Contains(log, substr) {
			return true
		}
	}
	return false
}

func TestPipeline_Success(t *testing.T) {
	taskA := successfulTask(t, "A", 10*time.Millisecond)
	taskB := successfulTask(t, "B", 10*time.Millisecond, taskA)
	taskC := successfulTask(t, "C", 10*time.Millisecond, taskA)
	taskD := successfulTask(t, "D", 10*time.Millisecond, taskB, taskC)

	logger := &testLogger{}
	pipeline, err := NewPipeline(Options{
		Name:   "SuccessTest",
		Tasks:  []*Task{taskA, taskB, taskC, taskD},
		Logger: logger.Log,
	})
	require.NoError(t, err)

	ctx := context.Background()
	err = pipeline.Run(ctx)

	assert.NoError(t, err, "Pipeline should run successfully")

	logOutput := logger.String()
	assert.Contains(t, logOutput, "Task(A): Completed")
	assert.Contains(t, logOutput, "Task(B): Completed")
	assert.Contains(t, logOutput, "Task(C): Completed")
	assert.Contains(t, logOutput, "Task(D): Completed")
	assert.Contains(t, logOutput, "All tasks completed successfully")
}

func TestPipeline_Failure(t *testing.T) {
	taskA := successfulTask(t, "A", 10*time.Millisecond)
	taskB := failingTask(t, "B", "intentional failure", taskA)
	taskC := successfulTask(t, "C", 10*time.Millisecond, taskA)
	taskD := successfulTask(t, "D", 10*time.Millisecond, taskB, taskC)

	logger := &testLogger{}
	pipeline, err := NewPipeline(Options{
		Name:   "FailureTest",
		Tasks:  []*Task{taskA, taskB, taskC, taskD},
		Logger: logger.Log,
	})
	require.NoError(t, err)

	ctx := context.Background()
	err = pipeline.Run(ctx)

	require.Error(t, err, "Pipeline should return an error")
	assert.Contains(t, err.Error(), `pipeline "FailureTest" failed with 1 error(s)`, "Error message should indicate pipeline failure")
	assert.Contains(t, err.Error(), `map[Task(B):intentional failure]`, "Error message should contain the specific task error map")

	logOutput := logger.String()
	assert.Contains(t, logOutput, "Task(A): Completed")
	assert.Contains(t, logOutput, "Task(B): Failed: intentional failure")
	assert.Contains(t, logOutput, "Task(C): Completed")
	assert.Contains(t, logOutput, "Task(D): Prerequisite Task(B) did not complete successfully (intentional failure).")
	assert.Contains(t, logOutput, "Completed with 1 error(s):")
	assert.NotContains(t, logOutput, "Task(D): Starting...")
	assert.NotContains(t, logOutput, "Task(D): Completed")
	assert.NotContains(t, logOutput, "All tasks completed successfully.")
}

func TestPipeline_CircularPrerequisite(t *testing.T) {
	// Define functions first
	noopFunc := func(ctx context.Context) error { return nil }

	// Need placeholder vars because we need the pointers for prerequisites.
	var taskA, taskB, taskC *Task
	var err error

	// Create tasks
	taskA, err = NewTask(TaskOptions{
		Name:          "A",
		Prerequisites: nil, // Prerequisites set later
		Function:      noopFunc,
	})
	require.NoError(t, err)

	taskB, err = NewTask(TaskOptions{
		Name:          "B",
		Prerequisites: []*Task{taskA}, // B -> A
		Function:      noopFunc,
	})
	require.NoError(t, err)

	// Create task C *before* setting it as a prerequisite
	taskC, err = NewTask(TaskOptions{
		Name:          "C",
		Prerequisites: []*Task{taskB}, // C -> B
		Function:      noopFunc,
	})
	require.NoError(t, err)

	// Manually create the circular prerequisite A -> C *after* all tasks exist.
	// This completes the cycle: A -> C -> B -> A
	// It accesses the unexported field directly, allowed within the same package.
	taskA.prerequisites = []*Task{taskC}

	logger := &testLogger{}
	// Pass all valid tasks to the pipeline
	pipeline, err := NewPipeline(Options{
		Name:   "CycleTest",
		Tasks:  []*Task{taskA, taskB, taskC},
		Logger: logger.Log,
	})
	require.NoError(t, err, "NewPipeline should not error on cycle definition, Run should")

	ctx := context.Background()
	err = pipeline.Run(ctx)

	require.Error(t, err, "Pipeline run should fail due to circular prerequisite")

	// Cycle path string depends on map iteration order and sorting in buildCyclePathString
	// Check for the core message and presence of involved tasks
	assert.Contains(t, err.Error(), "circular prerequisite detected:", "Error message should report cycle detection")
	// Check that all nodes are mentioned in the cycle path
	assert.Contains(t, err.Error(), "A", "Cycle path should contain A")
	assert.Contains(t, err.Error(), "B", "Cycle path should contain B")
	assert.Contains(t, err.Error(), "C", "Cycle path should contain C")
	assert.Contains(t, err.Error(), "->", "Cycle path should contain ->")

	logOutput := logger.String()
	assert.NotContains(t, logOutput, "Starting...")
	assert.NotContains(t, logOutput, "Completed")
}

func TestPipeline_ContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	blockerTaskStarted := make(chan struct{})
	blockerTaskFinished := make(chan struct{})

	taskA, errA := NewTask(TaskOptions{
		Name:          "Blocker",
		Prerequisites: nil,
		Function: func(taskCtx context.Context) error {
			close(blockerTaskStarted)
			select {
			case <-time.After(5 * time.Second):
				t.Error("Blocker task was not cancelled")
				return errors.New("not cancelled")
			case <-taskCtx.Done():
				close(blockerTaskFinished)
				return taskCtx.Err()
			}
		},
	})
	require.NoError(t, errA)

	taskB := successfulTask(t, "B", 10*time.Millisecond, taskA)

	logger := &testLogger{}
	pipeline, err := NewPipeline(Options{
		Name:   "CancelTest",
		Tasks:  []*Task{taskA, taskB},
		Logger: logger.Log,
	})
	require.NoError(t, err)

	var pipelineErr error
	pipelineDone := make(chan struct{})
	go func() {
		pipelineErr = pipeline.Run(ctx)
		close(pipelineDone)
	}()

	select {
	case <-blockerTaskStarted:

	case <-time.After(1 * time.Second):
		t.Fatal("Blocker task did not start in time")
	}

	cancel()

	select {
	case <-pipelineDone:

	case <-time.After(2 * time.Second):
		t.Fatal("Pipeline did not finish after cancellation")
	}

	verifyCancellationResults(t, pipelineErr, logger, blockerTaskFinished)
}

func verifyCancellationResults(t *testing.T, pipelineErr error, logger *testLogger, blockerTaskFinished chan struct{}) {
	t.Helper()

	select {
	case <-blockerTaskFinished:
		// Expected path
	case <-time.After(100 * time.Millisecond):
		t.Error("Blocker task did not signal it finished after cancellation")
	}

	require.Error(t, pipelineErr, "Pipeline should return an error due to context cancellation")
	assert.True(t, errors.Is(pipelineErr, context.Canceled) || errors.Is(pipelineErr, context.DeadlineExceeded), "Error should be context.Canceled or DeadlineExceeded")

	logOutput := logger.String()
	assert.Contains(t, logOutput, "Task(Blocker): Skipping: task skipped due to prerequisite failure or cancellation: context canceled")
	assert.Contains(t, logOutput, "Task(B): Skipping:")
	assert.Contains(t, logOutput, "External context cancelled. Waiting for tasks to finish...")
	assert.Contains(t, logOutput, "Tasks finished after cancellation signal.")
	assert.NotContains(t, logOutput, "Task(B): Starting...")
	assert.NotContains(t, logOutput, "Task(B): Completed")
	assert.NotContains(t, logOutput, "All tasks completed successfully.")
}

func TestPipeline_PrerequisiteFailed(t *testing.T) {
	taskA := failingTask(t, "A", "root cause failure")
	taskB := successfulTask(t, "B", 10*time.Millisecond, taskA)
	taskC := successfulTask(t, "C", 10*time.Millisecond, taskB)

	logger := &testLogger{}
	pipeline, err := NewPipeline(Options{
		Name:   "DepFailTest",
		Tasks:  []*Task{taskA, taskB, taskC},
		Logger: logger.Log,
	})
	require.NoError(t, err)

	ctx := context.Background()
	err = pipeline.Run(ctx)

	require.Error(t, err)
	assert.Contains(t, err.Error(), `pipeline "DepFailTest" failed with 1 error(s):`, "Error message should indicate failure")
	assert.Contains(t, err.Error(), `map[Task(A):root cause failure]`, "Error message should contain the specific task error")

	logOutput := logger.String()
	assert.Contains(t, logOutput, "Task(A): Failed: root cause failure")
	assert.Contains(t, logOutput, "Task(B): Skipping: task skipped due to prerequisite failure or cancellation: root cause failure")
	assert.NotContains(t, logOutput, "Task(B): Starting...")
	assert.NotContains(t, logOutput, "Task(B): Completed")
}

func TestPipeline_MissingPrerequisiteDefinition(t *testing.T) {
	mockDep := &Task{name: "Ghost"} // Can use unexported field within same package test
	taskA, errA := NewTask(TaskOptions{
		Name:          "A",
		Prerequisites: []*Task{mockDep},
		Function:      func(ctx context.Context) error { return nil },
	})
	require.NoError(t, errA)

	logger := &testLogger{}
	pipeline, err := NewPipeline(Options{
		Name:   "MissingDepDefTest",
		Tasks:  []*Task{taskA},
		Logger: logger.Log,
	})
	require.NoError(t, err, "NewPipeline should not error on missing prerequisite, Run should")

	ctx := context.Background()
	err = pipeline.Run(ctx)

	require.Error(t, err, "Pipeline run should fail due to missing prerequisite")
	assert.Contains(t, err.Error(), `prerequisite "Ghost" for task "A" not found`, "Error message should report the missing prerequisite")

	logOutput := logger.String()
	assert.NotContains(t, logOutput, "Starting...")
	assert.NotContains(t, logOutput, "Completed")
}

func TestPipeline_NoTasks(t *testing.T) {
	logger := &testLogger{}
	_, err := NewPipeline(Options{
		Name:   "NoTasksTest",
		Tasks:  nil,
		Logger: logger.Log,
	})
	require.Error(t, err)
	assert.EqualError(t, err, "pipeline options must include all of the pipeline's tasks")

	_, err = NewPipeline(Options{
		Name:   "EmptyTasksTest",
		Tasks:  []*Task{},
		Logger: logger.Log,
	})

	require.NoError(t, err)

	pipeline, _ := NewPipeline(Options{
		Name:   "EmptyTasksTest",
		Tasks:  []*Task{},
		Logger: logger.Log,
	})
	runErr := pipeline.Run(context.Background())
	assert.NoError(t, runErr)
	assert.Contains(t, logger.String(), "All tasks completed successfully")
}

func TestPipeline_DefaultLogger(t *testing.T) {

	taskA := successfulTask(t, "A", 5*time.Millisecond)
	p, err := NewPipeline(Options{
		Name:   "DefaultLoggerTest",
		Tasks:  []*Task{taskA},
		Logger: nil,
	})
	require.NoError(t, err, "NewPipeline should succeed with nil logger")
	require.NotNil(t, p.logger, "Pipeline logger should be set to a default function")

	ctx := context.Background()
	err = p.Run(ctx)
	assert.NoError(t, err, "Pipeline run should succeed with the default logger")
}

func TestPipeline_ContextCancellationBeforeTaskStart(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	taskA := successfulTask(t, "A", 5*time.Second) // Long duration, should not run

	logger := &testLogger{}
	pipeline, err := NewPipeline(Options{
		Name:   "PreCancelTest",
		Tasks:  []*Task{taskA},
		Logger: logger.Log,
	})
	require.NoError(t, err)

	pipelineErr := pipeline.Run(ctx)

	assert.ErrorIs(t, pipelineErr, context.Canceled, "Pipeline should return context.Canceled error")

	logOutput := logger.String()
	fmt.Println(logOutput)

	assert.Contains(t, logOutput, "Task(A): Skipping:", "Log should show cancellation before execution")
	assert.Contains(t, logOutput, "External context cancelled. Waiting for tasks to finish...", "Log should show external context was cancelled")
	assert.NotContains(t, logOutput, "Task(A): Starting...")
	assert.NotContains(t, logOutput, "Task(A): Completed")
	assert.NotContains(t, logOutput, "All tasks completed successfully")
}

func TestPipeline_PanicRecovery(t *testing.T) {
	taskA := successfulTask(t, "A", 10*time.Millisecond)
	taskB := panicTask(t, "B", "intentional panic", taskA)
	taskC := successfulTask(t, "C", 10*time.Millisecond, taskA) // Should run
	taskD := successfulTask(t, "D", 10*time.Millisecond, taskB) // Should be skipped

	logger := &testLogger{}
	pipeline, err := NewPipeline(Options{
		Name:   "PanicTest",
		Tasks:  []*Task{taskA, taskB, taskC, taskD},
		Logger: logger.Log,
	})
	require.NoError(t, err)

	ctx := context.Background()
	err = pipeline.Run(ctx)

	require.Error(t, err, "Pipeline should return an error due to panic")
	assert.Contains(t, err.Error(), "pipeline \"PanicTest\" failed with 1 error(s)")
	assert.Contains(t, err.Error(), "map[Task(B):Task(B) panicked: intentional panic]")

	logOutput := logger.String()
	assert.Contains(t, logOutput, "Task(A): Completed")
	assert.Contains(t, logOutput, "Task(B): PANICKED: intentional panic")
	assert.Contains(t, logOutput, "Task(C): Completed") // C should still complete as it only depends on A
	assert.Contains(t, logOutput, "Task(D): Prerequisite Task(B) did not complete successfully (Task(B) panicked: intentional panic).")
	assert.Contains(t, logOutput, "Completed with 1 error(s):")
	assert.NotContains(t, logOutput, "Task(D): Starting...")
	assert.NotContains(t, logOutput, "All tasks completed successfully.")
}

func TestBuildCyclePathString_DefensiveCheck(t *testing.T) {
	s1 := &Task{name: "S1"}
	s2 := &Task{name: "S2"}
	s3 := &Task{name: "S3"}

	path := []*Task{s1, s2}
	cycleStartNodeNotInPath := s3 // Node not in the path
	result := buildCyclePathString(path, cycleStartNodeNotInPath)
	assert.Contains(t, result, "internal error building cycle path", "Should return error message if start node not found")
}

func TestPipeline_IndependentBranchFailure(t *testing.T) {
	//    A
	//   / \
	//  B   C (fails)
	//  |   |
	//  D   E (skipped)
	//   \ /
	//    F (skipped)

	taskA := successfulTask(t, "A", 5*time.Millisecond)
	taskB := successfulTask(t, "B", 5*time.Millisecond, taskA)
	taskC := failingTask(t, "C", "C failed", taskA)
	taskD := successfulTask(t, "D", 5*time.Millisecond, taskB)
	taskE := successfulTask(t, "E", 5*time.Millisecond, taskC)
	taskF := successfulTask(t, "F", 5*time.Millisecond, taskD, taskE)

	logger := &testLogger{}
	pipeline, err := NewPipeline(Options{
		Name:   "BranchFailureTest",
		Tasks:  []*Task{taskA, taskB, taskC, taskD, taskE, taskF},
		Logger: logger.Log,
	})
	require.NoError(t, err, "Pipeline creation should succeed")

	ctx := context.Background()
	pipelineErr := pipeline.Run(ctx)

	require.Error(t, pipelineErr, "Pipeline run should fail because branch 1 failed")
	assert.Contains(t, pipelineErr.Error(), `pipeline "BranchFailureTest" failed with 1 error(s):`, "Error message should indicate the branch failure")
	assert.Contains(t, pipelineErr.Error(), `map[Task(C):C failed]`, "Error message should contain the specific task error")

	logOutput := logger.String()
	fmt.Println(logOutput)

	// Verify Branch 1 (Failure)
	assert.Contains(t, logOutput, "Task(A): Completed")
	assert.Contains(t, logOutput, "Task(B): Completed")
	assert.Contains(t, logOutput, "Task(C): Failed: C failed")
	assert.Contains(t, logOutput, "Task(D): Completed")
	assert.Contains(t, logOutput, "Task(E): Skipping: task skipped due to prerequisite failure or cancellation: C failed")
	assert.Contains(t, logOutput, "Task(F): Skipping: task skipped due to prerequisite failure or cancellation: task skipped due to prerequisite failure or cancellation: C failed")

	// Verify overall status
	assert.Contains(t, logOutput, "Completed with 1 error(s):")
	assert.NotContains(t, logOutput, "All tasks completed successfully")
}

// TestPipelineRaceCondition creates a simple DAG with fast-running tasks
// to increase the likelihood of exposing race conditions when run with the -race flag.
func TestPipelineRaceCondition(t *testing.T) {
	const numTasks = 50
	tasks := make([]*Task, numTasks)
	mu := sync.Mutex{}
	counter := 0

	for i := 0; i < numTasks; i++ {
		taskName := fmt.Sprintf("Task-%d", i)
		var err error
		tasks[i], err = NewTask(TaskOptions{
			Name:          taskName,
			Prerequisites: nil,
			Function: func(ctx context.Context) error {
				// Simulate some work and potential shared resource access
				time.Sleep(time.Duration(i%5+1) * time.Millisecond)
				mu.Lock()
				counter++
				mu.Unlock()
				return nil
			},
		})
		require.NoError(t, err)
	}

	logger := &testLogger{}
	pipeline, err := NewPipeline(Options{
		Name:   "RaceTest",
		Tasks:  tasks,
		Logger: logger.Log,
	})
	assert.NoError(t, err, "Pipeline creation should not fail")
	assert.NotNil(t, pipeline, "Pipeline should be created")

	err = pipeline.Run(context.Background())
	assert.NoError(t, err, "Pipeline execution should complete without error")
}

// TestPipeline_MultipleIndependentFailures tests a scenario where multiple tasks
// in independent branches fail. It verifies that all independent failures are
// reported and that dependent tasks are correctly skipped.
func TestPipeline_MultipleIndependentFailures(t *testing.T) {
	// Start -> A (fail) -> B (skip)
	// Start -> C (fail) -> D (skip)
	// Start -> E (ok)   -> F (ok)

	taskA := failingTask(t, "A", "Fail A")
	taskB := successfulTask(t, "B", 5*time.Millisecond, taskA)
	taskC := failingTask(t, "C", "Fail C")
	taskD := successfulTask(t, "D", 5*time.Millisecond, taskC)
	taskE := successfulTask(t, "E", 5*time.Millisecond)
	taskF := successfulTask(t, "F", 5*time.Millisecond, taskE)

	logger := &testLogger{}
	pipeline, err := NewPipeline(Options{
		Name:   "MultiFailTest",
		Tasks:  []*Task{taskA, taskB, taskC, taskD, taskE, taskF},
		Logger: logger.Log,
	})
	require.NoError(t, err, "Pipeline creation should succeed")

	ctx := context.Background()
	pipelineErr := pipeline.Run(ctx)

	require.Error(t, pipelineErr, "Pipeline run should fail because multiple branches failed")
	assert.Contains(t, pipelineErr.Error(), "failed with 2 error(s)", "Should report 2 actual failures")
	assert.Contains(t, pipelineErr.Error(), `map[Task(A):Fail A Task(C):Fail C]`, "Error message should contain the specific task error map")

	logOutput := logger.String()
	fmt.Println(logOutput) // Print logs for debugging

	// Verify failures
	assert.Contains(t, logOutput, "Task(A): Failed: Fail A")
	assert.Contains(t, logOutput, "Task(C): Failed: Fail C")

	// Verify skips due to failures
	assert.Contains(t, logOutput, "Task(B): Prerequisite Task(A) did not complete successfully")
	assert.Contains(t, logOutput, "Task(D): Prerequisite Task(C) did not complete successfully")
	assert.Contains(t, logOutput, "Task(F): Prerequisite Task(E) completed successfully")

	// Verify successes
	assert.Contains(t, logOutput, "Task(E): Completed")
	assert.Contains(t, logOutput, "Task(F): Completed")

	// Verify tasks that should not have started
	assert.NotContains(t, logOutput, "Task(B): Starting...")
	assert.NotContains(t, logOutput, "Task(D): Starting...")

	// Verify overall status
	assert.Contains(t, logOutput, "Completed with 2 error(s):")
	assert.NotContains(t, logOutput, "All tasks completed successfully")
}

// Helper function to wait for multiple channels with timeout
func waitForChannels(t *testing.T, channels map[string]chan struct{}, keys []string, timeout time.Duration, messageFormat string) {
	t.Helper()
	var wg sync.WaitGroup
	wg.Add(len(keys))
	errs := make(chan error, len(keys)) // Buffered channel for errors

	for _, key := range keys {
		key := key // Capture range variable for goroutine
		ch, ok := channels[key]
		if !ok {
			// Immediately fail if a channel is missing - should not happen with correct setup
			t.Fatalf("waitForChannels: Channel for key '%s' not found in map", key)
			return // Exit early
		}
		go func() {
			defer wg.Done()
			select {
			case <-ch:
				// Success, channel closed
			case <-time.After(timeout):
				// Timeout occurred
				errs <- fmt.Errorf(messageFormat, key)
			}
		}()
	}

	// Wait for all goroutines to complete
	wg.Wait()
	close(errs) // Close the error channel after all goroutines are done

	// Collect and report errors
	var combinedErrors []string
	for err := range errs {
		combinedErrors = append(combinedErrors, err.Error())
	}

	if len(combinedErrors) > 0 {
		t.Fatalf("waitForChannels timed out for: %s", strings.Join(combinedErrors, "; "))
	}
}

// Helper function to create a standard test task that signals start/finish
// Note: This helper assumes the task function itself handles context cancellation appropriately.
func newTestTask(t *testing.T, name string, duration time.Duration, startedChans, finishedChans map[string]chan struct{}, prerequisites ...*Task) *Task {
	t.Helper()
	task, err := NewTask(TaskOptions{
		Name:          name,
		Prerequisites: prerequisites,
		Function: func(taskCtx context.Context) error {
			// Signal start
			if ch, ok := startedChans[name]; ok && ch != nil {
				// Use non-blocking send or close to prevent deadlock if channel is unbuffered or full
				// Closing is generally safer for signaling completion.
				close(ch)
			} else {
				t.Logf("Warning: startedChan not found or nil for task %s", name)
			}

			finishFunc := func() {
				// Signal finish
				if ch, ok := finishedChans[name]; ok && ch != nil {
					close(ch)
				} else {
					t.Logf("Warning: finishedChan not found or nil for task %s", name)
				}
			}
			defer finishFunc() // Ensure finish is always signaled

			// Simulate work or wait
			select {
			case <-time.After(duration):
				return nil
			case <-taskCtx.Done():
				return taskCtx.Err()
			}
		},
	})
	require.NoError(t, err, "Failed to create task %s", name)
	return task
}

// TestPipeline_ComplexCancellation tests cancellation while a task is waiting for multiple prerequisites,
// some of which might finish before cancellation.
func TestPipeline_ComplexCancellation(t *testing.T) {
	t.Parallel() // Mark test as parallelizable

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Setup channels
	startedChans := make(map[string]chan struct{})
	finishedChans := make(map[string]chan struct{})
	taskNames := []string{"Blocker", "A", "B", "C", "D"} // Order matters for prerequisite
	for _, name := range taskNames {
		// Use buffered channels size 1 to prevent blocking if the receiver isn't ready,
		// although closing is the primary mechanism here.
		startedChans[name] = make(chan struct{}, 1)
		finishedChans[name] = make(chan struct{}, 1)
	}

	// --- Task Creation ---
	blockerCanFinish := make(chan struct{})
	blocker, err := NewTask(TaskOptions{
		Name: "Blocker",
		Function: func(taskCtx context.Context) error {
			// Manually handle start/finish for the custom blocker logic
			close(startedChans["Blocker"])
			defer close(finishedChans["Blocker"]) // Ensure finished is closed
			select {
			case <-blockerCanFinish:
				return nil
			case <-taskCtx.Done():
				return taskCtx.Err()
			}
		},
	})
	require.NoError(t, err)

	// Use the helper for standard tasks
	taskA := newTestTask(t, "A", 100*time.Millisecond, startedChans, finishedChans, blocker)
	taskB := newTestTask(t, "B", 50*time.Millisecond, startedChans, finishedChans, taskA)
	taskC := newTestTask(t, "C", 50*time.Millisecond, startedChans, finishedChans, taskA)
	taskD := newTestTask(t, "D", 50*time.Millisecond, startedChans, finishedChans, taskB) // Correct prerequisite

	// --- Pipeline Setup ---
	logger := &testLogger{}
	pipeline, err := NewPipeline(Options{
		Name:   "ComplexCancelRefactored", // Changed name slightly
		Tasks:  []*Task{blocker, taskA, taskB, taskC, taskD},
		Logger: logger.Log,
	})
	require.NoError(t, err)

	// --- Run Pipeline ---
	var pipelineErr error
	pipelineDone := make(chan struct{})
	go func() {
		defer close(pipelineDone)
		pipelineErr = pipeline.Run(ctx)
	}()

	// --- Test Execution Flow ---
	// Wait for Blocker start
	waitForChannels(t, startedChans, []string{"Blocker"}, 1*time.Second, "Task %s did not start in time")

	// Signal blocker to finish so dependent tasks can proceed
	close(blockerCanFinish)

	// Wait for Blocker to finish explicitly before waiting for subsequent tasks to start
	waitForChannels(t, finishedChans, []string{"Blocker"}, 1*time.Second, "Blocker task did not finish in time after being signaled")

	// Wait for dependent tasks to start
	startTimeout := 2 * time.Second // Increased timeout slightly as tasks run sequentially
	waitForChannels(t, startedChans, []string{"A", "B", "C", "D"}, startTimeout, "Task %s did not start in time")

	// Wait for *some* tasks to finish, then cancel
	waitForChannels(t, finishedChans, []string{"A"}, 1*time.Second, "Task %s did not finish in time")

	// Cancel context *while* other tasks (B, C, D) are likely running or queued
	t.Log("Cancelling context...")
	cancel()

	// Wait for pipeline goroutine to finish
	select {
	case <-pipelineDone:
	case <-time.After(2 * time.Second):
		t.Fatal("Pipeline did not finish after cancellation signal")
	}

	// --- Assertions ---
	// Even though tasks completed, the pipeline detects the external cancellation signal
	// before returning, resulting in a cancellation error.
	assert.ErrorIs(t, pipelineErr, context.Canceled, "Pipeline should return context.Canceled because the context was cancelled externally")

	logOutput := logger.String()
	t.Log(logOutput) // Log output for debugging

	// Verify that cancellation was logged
	assert.Contains(t, logOutput, "External context cancelled.", "Pipeline should log external cancellation")
	// Verify the overall status indicates cancellation, not success
	assert.NotContains(t, logOutput, "All tasks completed successfully", "Pipeline should not log successful completion after cancellation")

	// Verify Blocker and A completed (they finished before cancel)
	assert.Contains(t, logOutput, "Task(Blocker): Completed", "Blocker should complete before cancellation")
	assert.Contains(t, logOutput, "Task(A): Completed", "A should complete before cancellation")

	// Verify B, C, D were skipped or cancelled
	// They might log "Completed" if they finish *very* quickly after A but before the cancel signal
	// propagates, or they might log "Skipping". The crucial part is the pipeline error and overall logs.
	// So, we won't assert *strictly* on their completion/skip status, but check the overall outcome.
	// We already asserted the pipeline error is context.Canceled and the log contains the cancellation message.
}

func TestPipeline_ConcurrencyLimit(t *testing.T) {
	concurrencyLimit := 2
	numTasks := 5
	taskDuration := 100 * time.Millisecond

	var currentConcurrency atomic.Int32
	var maxConcurrency atomic.Int32

	tasks := make([]*Task, numTasks)
	for i := 0; i < numTasks; i++ {
		taskName := strconv.Itoa(i)
		tasks[i], _ = NewTask(TaskOptions{
			Name:          taskName,
			Prerequisites: nil,
			Function: func(ctx context.Context) error {
				// Increment current concurrency
				val := currentConcurrency.Add(1)
				// Update max concurrency if needed
				if currentVal := maxConcurrency.Load(); val > currentVal {
					// Use CompareAndSwap for atomicity, though simple Store might be okay here
					// if overshooting slightly due to race condition is acceptable for tracking max.
					// CAS is safer.
					maxConcurrency.CompareAndSwap(currentVal, val)
				}

				// Simulate work
				select {
				case <-time.After(taskDuration):
					// Decrement concurrency after work
					currentConcurrency.Add(-1)
					return nil
				case <-ctx.Done():
					currentConcurrency.Add(-1)
					return ctx.Err()
				}
			},
		})
	}

	logger := &testLogger{}
	pipeline, err := NewPipeline(Options{
		Name:             "ConcurrencyLimitTest",
		Tasks:            tasks,
		Logger:           logger.Log,
		ConcurrencyLimit: concurrencyLimit,
	})
	require.NoError(t, err)

	ctx := context.Background()
	startTime := time.Now()
	err = pipeline.Run(ctx)
	duration := time.Since(startTime)

	assert.NoError(t, err, "Pipeline should run successfully")

	maxObserved := maxConcurrency.Load()
	assert.LessOrEqual(t, int(maxObserved), concurrencyLimit, "Maximum concurrency should not exceed the limit")

	// Basic sanity check on duration: If all 5 ran sequentially, it would take 5 * 100ms = 500ms.
	// With concurrency 2, it should be roughly (5 / 2) * 100ms + overhead => ~300ms.
	// Allow significant buffer for test environment variability.
	expectedMinDuration := taskDuration * time.Duration((numTasks+concurrencyLimit-1)/concurrencyLimit) // Ceiling division
	assert.GreaterOrEqual(t, duration, expectedMinDuration, "Pipeline duration seems too short for the concurrency limit")
	assert.Less(t, duration, expectedMinDuration+2*taskDuration, "Pipeline duration seems too long") // Generous upper bound

	logOutput := logger.String()
	assert.Contains(t, logOutput, "All tasks completed successfully")
	for i := 0; i < numTasks; i++ {
		assert.Contains(t, logOutput, fmt.Sprintf("Task(%d): Completed", i))
	}
	fmt.Printf("Max concurrency observed: %d\n", maxObserved) // Print for info
}

// TestPipeline_NilPrerequisiteEntry verifies that the pipeline run fails if a task
// has a nil pointer in its prerequisite list.
func TestPipeline_NilPrerequisiteEntry(t *testing.T) {
	// Create a valid task that could be a prerequisite (though not strictly necessary for this test)
	taskA := successfulTask(t, "A", 1*time.Millisecond)

	// Create the task that will have the nil prerequisite
	taskB, errB := NewTask(TaskOptions{
		Name:     "B",
		Function: func(ctx context.Context) error { return nil },
		// Prerequisites set manually below
	})
	require.NoError(t, errB, "Creating task B failed")

	// Manually introduce a nil entry into the prerequisites list.
	// This accesses the unexported field, allowed within the same package test.
	taskB.prerequisites = []*Task{nil} // Just a nil entry is sufficient

	logger := &testLogger{}
	pipeline, err := NewPipeline(Options{
		Name:   "NilPrerequisiteTest",
		Tasks:  []*Task{taskA, taskB}, // Include both tasks
		Logger: logger.Log,
	})
	// Assuming NewPipeline doesn't do this deep check based on other tests
	require.NoError(t, err, "NewPipeline should succeed even with nil prerequisite entry (check happens at Run)")

	ctx := context.Background()
	runErr := pipeline.Run(ctx)

	// Assert that Run fails
	require.Error(t, runErr, "Pipeline Run should fail due to nil prerequisite entry")
	// Assert the specific error message
	expectedErrMsg := `task "B" has a nil prerequisite entry`
	assert.Contains(t, runErr.Error(), expectedErrMsg, "Error message should report the nil prerequisite entry")

	// Ensure no tasks were actually started
	logOutput := logger.String()
	assert.NotContains(t, logOutput, "Starting...", "No task should have started")
	assert.NotContains(t, logOutput, "Completed", "No task should have completed")
	assert.NotContains(t, logOutput, "All tasks completed successfully")
}

func TestNewTask_Validation(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name        string
		opts        TaskOptions
		expectedErr string
	}{
		{
			name: "Empty Name",
			opts: TaskOptions{
				Name:     "",
				Function: func(ctx context.Context) error { return nil },
			},
			expectedErr: "task name cannot be empty",
		},
		{
			name: "Nil Function",
			opts: TaskOptions{
				Name:     "ValidName",
				Function: nil,
			},
			expectedErr: "task function cannot be nil",
		},
	}

	for _, tc := range testCases {
		tc := tc // Capture range variable
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			task, err := NewTask(tc.opts)
			require.Error(t, err)
			assert.Nil(t, task)
			assert.Contains(t, err.Error(), tc.expectedErr)
		})
	}
}

func TestPipelineDefaultRetryStrategy(t *testing.T) {
	t.Parallel()

	var attempts atomic.Int32
	var taskExecutions atomic.Int32
	var retryStrategyCalls atomic.Int32

	retryStrategy := func(err error, attempt int) error {
		retryStrategyCalls.Add(1)
		assert.Equal(t, int(attempts.Load()), attempt, "Incorrect attempt number passed to retry strategy")
		attempts.Add(1)
		if attempt >= 2 { // Retry up to 2 times (3 attempts total)
			return fmt.Errorf("giving up after %d attempts: %w", attempt+1, err) // Return the original error or a new one
		}
		time.Sleep(10 * time.Millisecond) // Simulate backoff
		return nil                        // Indicate retry should happen
	}

	var failCount atomic.Int32
	failCount.Store(3) // Fail 3 times

	taskA, err := NewTask(TaskOptions{
		Name: "A",
		Function: func(ctx context.Context) error {
			taskExecutions.Add(1)
			if failCount.Load() > 0 {
				failCount.Add(-1)
				return fmt.Errorf("task A intentional fail %d", 3-failCount.Load())
			}
			return nil // Success eventually
		},
	})
	require.NoError(t, err)

	p, err := NewPipeline(Options{
		Name:                 "RetryPipeline",
		Tasks:                []*Task{taskA},
		DefaultRetryStrategy: retryStrategy,
	})
	require.NoError(t, err)

	// Case 1: Task fails more times than retries allowed
	err = p.Run(context.Background())
	require.Error(t, err, "Pipeline should fail if retries are exhausted")
	assert.Contains(t, err.Error(), "giving up after 3 attempts", "Error message should indicate giving up")
	assert.Equal(t, int32(3), taskExecutions.Load(), "Task should execute 3 times (1 initial + 2 retries)")
	assert.Equal(t, int32(3), retryStrategyCalls.Load(), "Retry strategy should be called 3 times")

	// Reset counters for next case
	attempts.Store(0)
	taskExecutions.Store(0)
	retryStrategyCalls.Store(0)
	failCount.Store(1) // Fail 1 time

	// Case 2: Task fails once, succeeds on retry
	err = p.Run(context.Background())
	require.NoError(t, err, "Pipeline should succeed if retry is successful")
	assert.Equal(t, int32(2), taskExecutions.Load(), "Task should execute 2 times (1 initial + 1 retry)")
	assert.Equal(t, int32(1), retryStrategyCalls.Load(), "Retry strategy should be called once")

	// Reset counters for next case
	attempts.Store(0)
	taskExecutions.Store(0)
	retryStrategyCalls.Store(0)
	failCount.Store(0) // Succeed immediately

	// Case 3: Task succeeds on first try
	err = p.Run(context.Background())
	require.NoError(t, err, "Pipeline should succeed if task succeeds immediately")
	assert.Equal(t, int32(1), taskExecutions.Load(), "Task should execute 1 time")
	assert.Equal(t, int32(0), retryStrategyCalls.Load(), "Retry strategy should not be called")
}

func TestTaskRetryStrategy(t *testing.T) {
	t.Parallel()

	var taskAttempts atomic.Int32
	var taskExecutions atomic.Int32
	var taskRetryStrategyCalls atomic.Int32

	taskRetryStrategy := func(err error, attempt int) error {
		taskRetryStrategyCalls.Add(1)
		assert.Equal(t, int(taskAttempts.Load()), attempt, "Incorrect attempt number passed to task retry strategy")
		taskAttempts.Add(1)
		if attempt >= 1 { // Retry only once (2 attempts total)
			return fmt.Errorf("task giving up after %d attempts: %w", attempt+1, err)
		}
		time.Sleep(10 * time.Millisecond)
		return nil
	}

	var failCount atomic.Int32
	failCount.Store(2) // Fail 2 times

	taskA, err := NewTask(TaskOptions{
		Name: "A",
		Function: func(ctx context.Context) error {
			taskExecutions.Add(1)
			if failCount.Load() > 0 {
				failCount.Add(-1)
				return fmt.Errorf("task A intentional fail %d", 2-failCount.Load())
			}
			return nil
		},
		RetryStrategy: taskRetryStrategy, // Task-specific strategy
	})
	require.NoError(t, err)

	p, err := NewPipeline(Options{
		Name:  "TaskRetry",
		Tasks: []*Task{taskA},
		// No default pipeline strategy
	})
	require.NoError(t, err)

	// Case 1: Task fails more times than its strategy allows
	err = p.Run(context.Background())
	require.Error(t, err, "Pipeline should fail if task retries are exhausted")
	assert.Contains(t, err.Error(), "task giving up after 2 attempts", "Error message should indicate task giving up")
	assert.Equal(t, int32(2), taskExecutions.Load(), "Task should execute 2 times (1 initial + 1 retry)")
	assert.Equal(t, int32(2), taskRetryStrategyCalls.Load(), "Task retry strategy should be called twice")

	// Reset counters
	taskAttempts.Store(0)
	taskExecutions.Store(0)
	taskRetryStrategyCalls.Store(0)
	failCount.Store(1) // Fail 1 time

	// Case 2: Task fails once, succeeds on retry
	err = p.Run(context.Background())
	require.NoError(t, err, "Pipeline should succeed if task retry is successful")
	assert.Equal(t, int32(2), taskExecutions.Load(), "Task should execute 2 times (1 initial + 1 retry)")
	assert.Equal(t, int32(1), taskRetryStrategyCalls.Load(), "Task retry strategy should be called once")
}

func TestRetryStrategyPrecedence(t *testing.T) {
	t.Parallel()

	var pipelineRetryCalls atomic.Int32
	var taskRetryCalls atomic.Int32
	var taskExecutions atomic.Int32

	pipelineRetryStrategy := func(err error, attempt int) error {
		pipelineRetryCalls.Add(1)
		return err // Pipeline strategy: fail immediately
	}

	taskRetryStrategy := func(err error, attempt int) error {
		taskRetryCalls.Add(1)
		if attempt == 0 {
			return nil // Task strategy: retry once
		}
		return err // Fail on second attempt
	}

	var failCount atomic.Int32
	failCount.Store(1) // Fail 1 time

	taskA, err := NewTask(TaskOptions{
		Name: "A",
		Function: func(ctx context.Context) error {
			taskExecutions.Add(1)
			if failCount.Load() > 0 {
				failCount.Add(-1)
				return errors.New("task A intentional fail")
			}
			return nil // Succeeds on retry
		},
		RetryStrategy: taskRetryStrategy, // Task-specific strategy
	})
	require.NoError(t, err)

	p, err := NewPipeline(Options{
		Name:                 "PrecedenceTest",
		Tasks:                []*Task{taskA},
		DefaultRetryStrategy: pipelineRetryStrategy, // Define a default strategy
	})
	require.NoError(t, err)

	// Run the pipeline
	err = p.Run(context.Background())
	require.NoError(t, err, "Pipeline should succeed because task retry succeeded")

	// Assertions
	assert.Equal(t, int32(0), pipelineRetryCalls.Load(), "Pipeline default retry strategy should NOT be called")
	assert.Equal(t, int32(1), taskRetryCalls.Load(), "Task retry strategy should be called once")
	assert.Equal(t, int32(2), taskExecutions.Load(), "Task should execute twice (initial + retry)")
}

func TestRetryStrategyContextCancellation(t *testing.T) {
	t.Parallel()

	var taskExecutions atomic.Int32
	var retryStrategyCalls atomic.Int32
	cancelCtx, cancel := context.WithCancel(context.Background())

	retryStrategy := func(err error, attempt int) error {
		retryStrategyCalls.Add(1)
		// Cancel context *during* the retry strategy's "wait"
		if attempt == 0 {
			cancel()
		}
		time.Sleep(50 * time.Millisecond) // Simulate backoff/wait where cancellation can occur
		if attempt >= 2 {                 // Allow up to 2 retries
			return err
		}
		return nil // Try again
	}

	var failCount atomic.Int32
	failCount.Store(5) // Set to fail more times than retries allow

	taskA, err := NewTask(TaskOptions{
		Name: "A",
		Function: func(ctx context.Context) error {
			// Check context before potentially long operation
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			taskExecutions.Add(1)
			// Simulate work
			time.Sleep(10 * time.Millisecond)

			// Check context again after work
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			if failCount.Load() > 0 {
				failCount.Add(-1)
				return errors.New("task A intentional fail")
			}
			return nil
		},
	})
	require.NoError(t, err)

	p, err := NewPipeline(Options{
		Name:                 "RetryCancel",
		Tasks:                []*Task{taskA},
		DefaultRetryStrategy: retryStrategy,
	})
	require.NoError(t, err)

	// Run the pipeline with the cancellable context
	err = p.Run(cancelCtx)

	// Assertions
	require.Error(t, err, "Pipeline run should return an error due to cancellation")
	// The exact error might be the context error or a skip error wrapping it
	assert.Contains(t, err.Error(), context.Canceled.Error(), "Error should indicate context cancellation")

	// Check how many times things were called
	// The task should execute once initially.
	// The retry strategy should be called once (attempt 0).
	// During the retry strategy's sleep, the context is cancelled.
	// The retry loop in runTaskWithRetry should detect cancellation *before* the next attempt.
	assert.Equal(t, int32(1), taskExecutions.Load(), "Task should execute only once before cancellation")
	assert.Equal(t, int32(1), retryStrategyCalls.Load(), "Retry strategy should be called once before cancellation")
}

func TestRetryStrategyPreventsInfiniteLoops(t *testing.T) {
	attempts := 0
	infiniteRetryStrategy := func(err error, attempt int) error {
		attempts++
		return nil
	}
	task, _ := NewTask(TaskOptions{
		Name: "A",
		Function: func(ctx context.Context) error {
			return errors.New("task A intentional fail")
		},
		RetryStrategy: infiniteRetryStrategy,
	})
	p, _ := NewPipeline(Options{
		Name:                 "RetryMaximumDepth",
		Tasks:                []*Task{task},
		DefaultRetryStrategy: infiniteRetryStrategy,
	})
	err := p.Run(context.Background())
	require.Error(t, err, "Pipeline run should return an error due to maximum retry depth")
	assert.Contains(t, err.Error(), "maximum retry depth reached", "Error should indicate maximum retry depth reached")
	assert.Equal(t, 1001, attempts, "Retry strategy should have been called >1000 times")
}

func TestRetryStrategySetMaximumDepth(t *testing.T) {
	attempts := 0
	maximumRetryDepth := 42
	infiniteRetryStrategy := func(err error, attempt int) error {
		attempts++
		return nil
	}
	task, _ := NewTask(TaskOptions{
		Name: "A",
		Function: func(ctx context.Context) error {
			return errors.New("task A intentional fail")
		},
		RetryStrategy: infiniteRetryStrategy,
	})
	p, _ := NewPipeline(Options{
		Name:                      "RetryMaximumDepth",
		Tasks:                     []*Task{task},
		DefaultRetryStrategy:      infiniteRetryStrategy,
		MaximumRetryStrategyDepth: &maximumRetryDepth,
	})
	err := p.Run(context.Background())
	require.Error(t, err, "Pipeline run should return an error due to maximum retry depth")
	assert.Contains(t, err.Error(), "maximum retry depth reached", "Error should indicate maximum retry depth reached")
	assert.Equal(t, 43, attempts, "Retry strategy should have been called 43 times")
}

func TestRetryStrategyPermitsInfiniteLoopOnNegativeMaximumDepth(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	attempts := 0
	maximumRetryDepth := -1 // -1 means no maximum retry depth
	infiniteRetryStrategy := func(err error, attempt int) error {
		attempts++
		return nil
	}
	task, _ := NewTask(TaskOptions{
		Name: "A",
		Function: func(ctx context.Context) error {
			return errors.New("task A intentional fail")
		},
		RetryStrategy: infiniteRetryStrategy,
	})
	p, _ := NewPipeline(Options{
		Name:                      "RetryMaximumDepth",
		Tasks:                     []*Task{task},
		DefaultRetryStrategy:      infiniteRetryStrategy,
		MaximumRetryStrategyDepth: &maximumRetryDepth,
	})
	err := p.Run(ctx)
	assert.Greater(t, attempts, 1000)
	assert.Error(t, err)
	assert.NotContains(t, err.Error(), "maximum retry depth reached", "Error should not indicate maximum retry depth reached")
	assert.Contains(t, err.Error(), "context deadline exceeded", "Error should indicate that the loop was ended prematurely and would've continued")
}

// TestPipeline_ConcurrencyLimitCancelWait tests the specific scenario where the context
// is cancelled while a task is waiting to acquire a concurrency slot.
func TestPipeline_ConcurrencyLimitCancelWait(t *testing.T) {
	concurrencyLimit := 1
	taskADuration := 50 * time.Millisecond // Long enough to hold the concurrency slot

	taskA := successfulTask(t, "A", taskADuration)
	// Remove prerequisite: Task B should try to run concurrently and get blocked by the limiter
	taskB := successfulTask(t, "B", 10*time.Millisecond /* no prerequisites */)

	logger := &testLogger{}
	pipeline, err := NewPipeline(Options{
		Name:             "ConcurrencyLimitCancelWait",
		Tasks:            []*Task{taskA, taskB},
		Logger:           logger.Log,
		ConcurrencyLimit: concurrencyLimit,
	})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())

	var pipelineErr error
	pipelineDone := make(chan struct{})

	go func() {
		defer close(pipelineDone)
		pipelineErr = pipeline.Run(ctx)
	}()

	time.Sleep(5 * time.Millisecond)
	cancel()

	<-pipelineDone

	// Assertions
	require.Error(t, pipelineErr, "Pipeline should return an error due to cancellation")
	// Check if the error is specifically context.Canceled
	assert.True(t, errors.Is(pipelineErr, context.Canceled), "Expected context.Canceled error, got: %v", pipelineErr)

	logOutput := logger.String()
	t.Log(logOutput) // Log output for debugging

	// Check pipeline error
	require.Error(t, pipelineErr, "Pipeline should return an error due to cancellation")
	assert.True(t, errors.Is(pipelineErr, context.Canceled), "Expected context.Canceled error, got: %v", pipelineErr)

	// Check general cancellation log
	assert.Contains(t, logOutput, "[Pipeline ConcurrencyLimitCancelWait] External context cancelled.")

	// Check which task started and which was skipped
	taskAStarted := strings.Contains(logOutput, "Task(A): Starting...")
	taskBStarted := strings.Contains(logOutput, "Task(B): Starting...")

	// Define the specific skip log pattern we expect for the waiting/cancelled task
	taskASkippedLog := "Task(A): Skipping: task skipped due to prerequisite failure or cancellation: context canceled"
	taskBSkippedLog := "Task(B): Skipping: task skipped due to prerequisite failure or cancellation: context canceled"

	taskASkipped := strings.Contains(logOutput, taskASkippedLog)
	taskBSkipped := strings.Contains(logOutput, taskBSkippedLog)

	// Assert that exactly one task started, and the *other* task was skipped due to cancellation.
	// The task that started might also get cancelled mid-execution and log a skip message, which is okay.
	aStartedAndBSkipped := taskAStarted && !taskBStarted && taskBSkipped
	bStartedAndASkipped := !taskAStarted && taskBStarted && taskASkipped

	assert.True(t, aStartedAndBSkipped || bStartedAndASkipped,
		"Expected exactly one task to start and the other to be skipped due to cancellation. Got: A started=%v, B started=%v, A skipped=%v, B skipped=%v",
		taskAStarted, taskBStarted, taskASkipped, taskBSkipped)

	// Optional: Ensure the task that was supposed to be skipped *waiting* didn't somehow complete
	if !taskAStarted && taskASkipped { // A was the one skipped while waiting
		assert.NotContains(t, logOutput, "Task(A): Completed", "Task A should not complete if it was skipped waiting")
	}
	if !taskBStarted && taskBSkipped { // B was the one skipped while waiting
		assert.NotContains(t, logOutput, "Task(B): Completed", "Task B should not complete if it was skipped waiting")
	}
}

// TestPipeline_CancelWhileWaitingForPrerequisite tests that cancellation is handled
// correctly when a task is actively waiting for a prerequisite.
func TestPipeline_CancelWhileWaitingForPrerequisite(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // Ensure cancellation happens eventually if test fails

	blockerStarted := make(chan struct{})
	blockerCanFinish := make(chan struct{}) // Never closed in this test
	waiterFuncStarted := make(chan struct{})

	// Task A: Blocks until signalled (which won't happen)
	taskBlocker, err := NewTask(TaskOptions{
		Name: "Blocker",
		Function: func(taskCtx context.Context) error {
			close(blockerStarted)
			t.Log("Blocker task started, waiting for signal or cancellation...")
			select {
			case <-blockerCanFinish:
				t.Log("Blocker unexpectedly allowed to finish")
				return nil
			case <-taskCtx.Done():
				t.Logf("Blocker received cancellation: %v", taskCtx.Err())
				return taskCtx.Err() // Correctly return context error
			}
		},
	})
	require.NoError(t, err)

	// Task B: Waits for Task A
	taskWaiter, err := NewTask(TaskOptions{
		Name:          "Waiter",
		Prerequisites: []*Task{taskBlocker},
		Function: func(taskCtx context.Context) error {
			// This part should ideally not be reached if cancellation during wait works
			t.Log("Waiter task function unexpectedly started execution")
			close(waiterFuncStarted) // Signal that the Waiter task *logic* has started
			return errors.New("waiter function started unexpectedly")
		},
	})
	require.NoError(t, err)

	logger := &testLogger{}
	pipeline, err := NewPipeline(Options{
		Name:   "CancelWhileWaitingTest",
		Tasks:  []*Task{taskBlocker, taskWaiter},
		Logger: logger.Log,
		// Use ConcurrencyLimit 2+ to ensure both can start scheduling phase promptly
		ConcurrencyLimit: 2,
	})
	require.NoError(t, err)

	var pipelineErr error
	pipelineDone := make(chan struct{})
	go func() {
		defer close(pipelineDone)
		t.Log("Starting pipeline run...")
		pipelineErr = pipeline.Run(ctx)
		t.Logf("Pipeline run finished with err: %v", pipelineErr)
	}()

	// Wait until the log confirms Waiter is waiting for Blocker.
	// This is the most reliable way to know it's in the target state without modifying pipeline internals.
	expectedLog := "Task(Waiter): Waiting for prerequisite Task(Blocker)..."
	logWaitTimeout := 3 * time.Second // Increased timeout slightly
	logCheckInterval := 10 * time.Millisecond
	startTime := time.Now()
	foundLog := false
	t.Logf("Waiting for log message: '%s'", expectedLog)
	for time.Since(startTime) < logWaitTimeout {
		if logger.Contains(expectedLog) {
			t.Logf("Found expected log message after %v.", time.Since(startTime))
			foundLog = true
			break
		}
		time.Sleep(logCheckInterval)
	}
	// Add a small delay AFTER finding the log to increase chance the select is entered
	// This is still a bit racy but better than nothing. Ideally, we'd have an internal hook.
	if foundLog {
		time.Sleep(20 * time.Millisecond)
	}

	require.True(t, foundLog, "Did not find log message '%s' within %v. Logs:\n%s", expectedLog, logWaitTimeout, logger.String())

	// Now that we know Waiter is waiting, cancel the context
	t.Logf("Cancelling context...")
	cancel()

	// Wait for the pipeline to finish
	select {
	case <-pipelineDone:
		t.Log("Pipeline finished.")
	case <-time.After(2 * time.Second):
		// If this timeout hits, it might indicate a deadlock or that cancellation wasn't handled properly.
		t.Fatalf("Pipeline did not finish within 2s after cancellation. Logs:\n%s", logger.String())
	}

	// Assertions
	require.Error(t, pipelineErr, "Pipeline should return an error")
	assert.ErrorIs(t, pipelineErr, context.Canceled, "Pipeline error should be context.Canceled")

	logOutput := logger.String()
	t.Logf("--- Final Logs ---:\n%s\n------------------", logOutput)

	// Verify the specific cancellation message *during* the wait
	assert.Contains(t, logOutput, "Task(Waiter): Cancelled by context while waiting for prerequisite Task(Blocker).", "Waiter should log cancellation during prerequisite wait")

	// Verify Waiter did not start its main function (it shouldn't have been closed)
	select {
	case <-waiterFuncStarted:
		t.Error("Waiter task function should not have started execution")
	default:
		// Expected: waiterFuncStarted channel remains open and unreadable
	}

	// Verify Blocker was also likely cancelled or skipped
	// Blocker might be skipped *before* starting if cancellation is fast, or fail *during* execution.
	blockerOutcomeLogged := strings.Contains(logOutput, "Task(Blocker): Skipping:") ||
		strings.Contains(logOutput, "Task(Blocker): Failed: context canceled") ||
		strings.Contains(logOutput, "Blocker received cancellation") // Log from task func
	assert.True(t, blockerOutcomeLogged, "Blocker task should show some form of cancellation in logs")

	assert.Contains(t, logOutput, "External context cancelled.")
	assert.NotContains(t, logOutput, "All tasks completed successfully")
}

// TestPipeline_SkipCondition tests that a task with a true SkipCondition is skipped
// and its dependent tasks still run.
func TestPipeline_SkipCondition(t *testing.T) {
	shouldSkip := true
	shouldSkipFunc := func() bool {
		return shouldSkip
	}

	taskA, errA := NewTask(TaskOptions{
		Name:          "Skippable",
		Prerequisites: nil,
		Function: func(ctx context.Context) error {
			// This function should not be called
			assert.Fail(t, "Skippable task function should not be executed")
			return errors.New("should have been skipped")
		},
		ShouldSkip: shouldSkipFunc,
	})
	require.NoError(t, errA)

	taskB := successfulTask(t, "Dependent", 10*time.Millisecond, taskA)

	logger := &testLogger{}
	pipeline, err := NewPipeline(Options{
		Name:   "SkipTest",
		Tasks:  []*Task{taskA, taskB},
		Logger: logger.Log,
	})
	require.NoError(t, err)

	ctx := context.Background()
	err = pipeline.Run(ctx)

	assert.NoError(t, err, "Pipeline should run successfully even with a skipped task")

	logOutput := logger.String()
	t.Logf("Logs:\n%s", logOutput) // Log output for debugging
	assert.Contains(t, logOutput, "Task(Skippable): Skipping due to skip condition.", "Skippable task should log skipping")
	assert.NotContains(t, logOutput, "Task(Skippable): Starting...", "Skippable task should not start execution")
	assert.NotContains(t, logOutput, "Task(Skippable): Completed", "Skippable task should not log completion")
	assert.Contains(t, logOutput, "Task(Dependent): Waiting for prerequisite Task(Skippable)...", "Dependent task should wait for Skippable")
	assert.Contains(t, logOutput, "Task(Dependent): Prerequisite Task(Skippable) completed successfully.", "Dependent task should see Skippable as successful")
	assert.Contains(t, logOutput, "Task(Dependent): Completed", "Dependent task should complete")
	assert.Contains(t, logOutput, "All tasks completed successfully")

	// Test again, but this time the condition is false
	shouldSkip = false
	taskAExecuted := atomic.Bool{}
	taskA.function = func(ctx context.Context) error {
		taskAExecuted.Store(true)
		return nil // Now it should execute and succeed
	}

	logger = &testLogger{} // Reset logger
	pipeline, err = NewPipeline(Options{
		Name:   "SkipTest-NotSkipped",
		Tasks:  []*Task{taskA, taskB},
		Logger: logger.Log,
	})
	require.NoError(t, err)

	err = pipeline.Run(ctx)
	assert.NoError(t, err, "Pipeline should run successfully when skip condition is false")
	assert.True(t, taskAExecuted.Load(), "Task A function should have been executed when skip condition is false")

	logOutput = logger.String()
	t.Logf("Logs (Not Skipped):\n%s", logOutput)
	assert.NotContains(t, logOutput, "Task(Skippable): Skipping due to skip condition.")
	assert.Contains(t, logOutput, "Task(Skippable): Starting...")
	assert.Contains(t, logOutput, "Task(Skippable): Completed")
	assert.Contains(t, logOutput, "Task(Dependent): Completed")
	assert.Contains(t, logOutput, "All tasks completed successfully")
}
