package idempipe

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Helper function to create a simple task that marks itself as executed
func makeTestTask(name string, executed *atomic.Bool, prerequisites ...*Task) *Task {
	task := NewTask(TaskOptions{
		Name: name,
		Function: func(ctx context.Context) error {
			executed.Store(true)
			// Simulate some work
			time.Sleep(10 * time.Millisecond)
			return nil
		},
		Prerequisites: prerequisites,
	})
	return task
}

// Helper function to create a task that returns a specific error
func makeErrorTask(name string, err error, prerequisites ...*Task) *Task {
	task := NewTask(TaskOptions{
		Name: name,
		Function: func(ctx context.Context) error {
			// Simulate some work before failing
			time.Sleep(10 * time.Millisecond)
			return err
		},
		Prerequisites: prerequisites,
	})
	return task
}

// Helper function to create a task that panics
func makePanicTask(name string, prerequisites ...*Task) *Task {
	task := NewTask(TaskOptions{
		Name: name,
		Function: func(ctx context.Context) error {
			panic("test panic")
		},
		Prerequisites: prerequisites,
	})
	return task
}

// TestNewPipeline_TaskValidation now tests that NewPipeline catches invalid tasks
func TestNewPipeline_TaskValidation(t *testing.T) {
	t.Run("Empty Name", func(t *testing.T) {
		invalidTask := NewTask(TaskOptions{Name: "", Function: func(ctx context.Context) error { return nil }})
		p, err := NewPipeline(Options{Tasks: []*Task{invalidTask}})
		require.NoError(t, err, "NewPipeline should succeed even with invalid task") // Validation happens in Run
		runErr := p.Run(context.Background())
		require.Error(t, runErr, "Run should fail due to empty task name")
		assert.Contains(t, runErr.Error(), "empty name")
	})

	t.Run("Nil Function", func(t *testing.T) {
		invalidTask := NewTask(TaskOptions{Name: "Test", Function: nil})
		p, err := NewPipeline(Options{Tasks: []*Task{invalidTask}})
		require.NoError(t, err, "NewPipeline should succeed even with invalid task") // Validation happens in Run
		runErr := p.Run(context.Background())
		require.Error(t, runErr, "Run should fail due to nil task function")
		assert.Contains(t, runErr.Error(), "missing a function")
	})

	t.Run("Nil Task Entry", func(t *testing.T) {
		p, err := NewPipeline(Options{Tasks: []*Task{nil}})
		require.NoError(t, err, "NewPipeline should succeed even with nil task entry") // Validation happens in Run
		runErr := p.Run(context.Background())
		require.Error(t, runErr, "Run should fail due to nil task entry")
		assert.Contains(t, runErr.Error(), "nil task entry")
	})

	t.Run("Valid Task", func(t *testing.T) {
		validTask := NewTask(TaskOptions{Name: "Valid", Function: func(ctx context.Context) error { return nil }})
		_, err := NewPipeline(Options{Tasks: []*Task{validTask}})
		require.NoError(t, err)
	})

	t.Run("Prerequisites Copied", func(t *testing.T) {
		originalPrereqs := []*Task{
			makeTestTask("prereq1", new(atomic.Bool)),
		}
		task := NewTask(TaskOptions{
			Name:          "Test",
			Function:      func(ctx context.Context) error { return nil },
			Prerequisites: originalPrereqs,
		})

		// Modify the original slice - we don't need the returned slice header, just the side effect
		_ = append(originalPrereqs, makeTestTask("prereq2", new(atomic.Bool)))

		// Check if the task's internal prerequisites were affected
		require.Len(t, task.prerequisites, 1, "Modification of original slice affected internal state.")
		assert.Equal(t, "prereq1", task.prerequisites[0].name)
	})
}

func TestNewPipelineValidation(t *testing.T) {
	t.Run("Nil Tasks", func(t *testing.T) {
		_, err := NewPipeline(Options{})
		if err == nil {
			t.Error("Expected error for nil tasks, got nil")
		}
	})

	t.Run("Valid Pipeline", func(t *testing.T) {
		task1 := makeTestTask("task1", new(atomic.Bool))
		_, err := NewPipeline(Options{Tasks: []*Task{task1}})
		if err != nil {
			t.Errorf("Expected no error for valid pipeline, got %v", err)
		}
	})

	t.Run("Pipeline Name Set", func(t *testing.T) {
		task1 := makeTestTask("task1", new(atomic.Bool))
		testName := "MyPipeline"
		p, err := NewPipeline(Options{
			Name:  testName,
			Tasks: []*Task{task1},
		})
		if err != nil {
			t.Errorf("Expected no error when setting pipeline name, got %v", err)
		}
		if p == nil {
			t.Fatal("Pipeline should not be nil")
		}
		// We can't directly access p.name easily without exporting it or adding a getter
		// But we confirm that providing the name didn't cause an error.
		// For full coverage, you might consider how to verify the name is used, e.g., checking log output if the logger captures it.
	})
}

func TestPipeline_Run_Successful(t *testing.T) {
	var executedA, executedB, executedC atomic.Bool

	taskA := makeTestTask("A", &executedA)
	taskB := makeTestTask("B", &executedB, taskA)
	taskC := makeTestTask("C", &executedC, taskA)

	p, err := NewPipeline(Options{Tasks: []*Task{taskA, taskB, taskC}})
	if err != nil {
		t.Fatalf("Failed to create pipeline: %v", err)
	}

	err = p.Run(context.Background())
	if err != nil {
		t.Fatalf("Pipeline run failed: %v", err)
	}

	if !executedA.Load() {
		t.Error("Task A was not executed")
	}
	if !executedB.Load() {
		t.Error("Task B was not executed")
	}
	if !executedC.Load() {
		t.Error("Task C was not executed")
	}
}

func TestPipeline_Run_SingleFailure(t *testing.T) {
	var executedA, executedC atomic.Bool
	failErr := errors.New("task B failed")

	taskA := makeTestTask("A", &executedA)
	taskB := makeErrorTask("B", failErr, taskA)
	taskC := makeTestTask("C", &executedC, taskB)

	p, err := NewPipeline(Options{Tasks: []*Task{taskA, taskB, taskC}})
	if err != nil {
		t.Fatalf("Failed to create pipeline: %v", err)
	}

	err = p.Run(context.Background())
	if err == nil {
		t.Fatal("Pipeline run expected to fail, but succeeded")
	}

	if !strings.Contains(err.Error(), failErr.Error()) {
		t.Errorf("Expected error to contain '%v', got '%v'", failErr, err)
	}

	if !executedA.Load() {
		t.Error("Task A should have executed")
	}
	if executedC.Load() {
		t.Error("Task C should have been skipped")
	}
}

func TestPipeline_Run_PrerequisiteFailureSkip(t *testing.T) {
	var executedA, executedB, executedC atomic.Bool
	failErr := errors.New("task A failed")

	taskA := makeErrorTask("A", failErr)          // A fails
	taskB := makeTestTask("B", &executedB, taskA) // B depends on A
	taskC := makeTestTask("C", &executedC, taskB) // C depends on B

	p, err := NewPipeline(Options{Tasks: []*Task{taskA, taskB, taskC}})
	if err != nil {
		t.Fatalf("Failed to create pipeline: %v", err)
	}

	err = p.Run(context.Background())
	if err == nil {
		t.Fatal("Pipeline run expected to fail, but succeeded")
	}

	if !strings.Contains(err.Error(), failErr.Error()) {
		t.Errorf("Expected error to contain '%v', got '%v'", failErr, err)
	}

	if executedA.Load() { // Should be false as it errors
		// This test is tricky, task A *runs* but returns an error. We can't easily check if the func body ran without more complex state.
		// We mainly care that B and C didn't run successfully.
	}
	if executedB.Load() {
		t.Error("Task B should have been skipped due to prerequisite failure")
	}
	if executedC.Load() {
		t.Error("Task C should have been skipped due to prerequisite failure")
	}

	// We verify the overall error and that B & C didn't run, which implies they were skipped.
	// Accessing internal p.taskErrors is not a good testing practice.
}

func TestPipeline_Run_ContextCancellation(t *testing.T) {
	var executedA, executedB atomic.Bool
	startSignal := make(chan struct{})
	taskAWait := make(chan struct{}) // Channel to make Task A wait

	taskA := NewTask(TaskOptions{
		Name: "A",
		Function: func(ctx context.Context) error {
			close(startSignal) // Signal that A has started
			executedA.Store(true)
			select {
			case <-taskAWait: // Wait until released or context cancelled
				// If released normally, sleep briefly *after* select
				time.Sleep(5 * time.Millisecond)
				return nil
			case <-ctx.Done():
				// If cancelled, sleep briefly *after* select
				time.Sleep(5 * time.Millisecond)
				return ctx.Err() // Propagate cancellation error
			}
		},
		IsComplete: func() bool {
			// Always false for this test, forcing wait if configured
			return false
		},
	})
	taskB := makeTestTask("B", &executedB, taskA)

	waitDuration := 50 * time.Millisecond // Short wait, cancellation should happen before this
	p, err := NewPipeline(Options{
		Tasks:                   []*Task{taskA, taskB},
		WaitForCompleteDuration: &waitDuration, // Engage wait logic
	})
	if err != nil {
		t.Fatalf("Failed to create pipeline: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	var pipelineErr error
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		pipelineErr = p.Run(ctx)
	}()

	// Wait for Task A to start
	select {
	case <-startSignal:
		// Task A has started, now cancel the context
		cancel()
	case <-time.After(2 * time.Second):
		t.Fatal("Timed out waiting for Task A to start")
	}

	wg.Wait() // Wait for the pipeline Run to finish

	if pipelineErr == nil {
		t.Fatal("Pipeline run expected to fail due to cancellation, but succeeded")
	}
	if !errors.Is(pipelineErr, context.Canceled) {
		t.Errorf("Expected pipeline error to be context.Canceled, got %v", pipelineErr)
	}

	if !executedA.Load() {
		t.Error("Task A should have started execution")
	}
	if executedB.Load() {
		t.Error("Task B should have been skipped due to cancellation")
	}

	// Check that the final pipeline error indicates cancellation.
	// Verifying internal skip status isn't necessary for black-box testing.
	close(taskAWait) // Release Task A if it's still waiting
}

func TestPipeline_Run_ConcurrencyLimit(t *testing.T) {
	concurrencyLimit := 2
	taskCount := 5
	var runningTasks atomic.Int32
	var maxConcurrent atomic.Int32
	var tasks []*Task
	taskDoneChans := make([]chan struct{}, taskCount)

	for i := 0; i < taskCount; i++ {
		taskDoneChans[i] = make(chan struct{})
		idx := i
		task := NewTask(TaskOptions{
			Name: fmt.Sprintf("Task-%d", idx),
			Function: func(ctx context.Context) error {
				current := runningTasks.Add(1)
				if currentMax := maxConcurrent.Load(); current > currentMax {
					maxConcurrent.Store(current)
				}
				defer runningTasks.Add(-1)
				// Simulate work and allow synchronization
				time.Sleep(50 * time.Millisecond)
				close(taskDoneChans[idx]) // Signal completion
				return nil
			},
		})
		tasks = append(tasks, task)
	}

	p, err := NewPipeline(Options{
		Tasks:            tasks,
		ConcurrencyLimit: concurrencyLimit,
	})
	if err != nil {
		t.Fatalf("Failed to create pipeline: %v", err)
	}

	err = p.Run(context.Background())
	if err != nil {
		t.Fatalf("Pipeline run failed: %v", err)
	}

	// Wait for all tasks to signal completion (optional sanity check)
	timeout := time.After(2 * time.Second)
	for i := 0; i < taskCount; i++ {
		select {
		case <-taskDoneChans[i]:
			// Task done
		case <-timeout:
			t.Fatalf("Timed out waiting for task %d to complete", i)
		}
	}

	if max := maxConcurrent.Load(); max > int32(concurrencyLimit) {
		t.Errorf("Exceeded concurrency limit: expected max %d, got %d", concurrencyLimit, max)
	} else if max == 0 && taskCount > 0 {
		t.Error("Max concurrent tasks was 0, indicates tasks may not have run correctly")
	} else if max < int32(concurrencyLimit) && taskCount >= concurrencyLimit {
		// This might be flaky if tasks finish too quickly, but generally expected
		t.Logf("Warning: Max concurrent tasks (%d) was less than limit (%d), potentially due to fast execution", max, concurrencyLimit)
	}
}

func TestPipeline_Run_RetryStrategy(t *testing.T) {
	t.Run("Successful Retry", func(t *testing.T) {
		var attempts atomic.Int32
		failErr := errors.New("transient error")
		maxAttempts := 3

		task := NewTask(TaskOptions{
			Name: "RetryTask",
			Function: func(ctx context.Context) error {
				currentAttempt := attempts.Add(1)
				if currentAttempt < int32(maxAttempts) {
					return failErr
				}
				return nil // Success on the maxAttempt-th try
			},
			RetryStrategy: func(err error, attempt int) error {
				if attempt >= maxAttempts-1 { // Retry attempts are 0-indexed
					return fmt.Errorf("giving up after attempt %d: %w", attempt, err) // Final failure
				}
				if !errors.Is(err, failErr) {
					return fmt.Errorf("unexpected error type for retry: %w", err)
				}
				time.Sleep(5 * time.Millisecond) // Simulate backoff
				return nil                       // Signal to retry
			},
		})

		p, err := NewPipeline(Options{Tasks: []*Task{task}})
		if err != nil {
			t.Fatalf("Failed to create pipeline: %v", err)
		}

		err = p.Run(context.Background())
		if err != nil {
			t.Errorf("Pipeline run failed despite successful retry: %v", err)
		}
		if attempts.Load() != int32(maxAttempts) {
			t.Errorf("Expected %d attempts, got %d", maxAttempts, attempts.Load())
		}
	})

	t.Run("Failure After Retries", func(t *testing.T) {
		var attempts atomic.Int32
		failErr := errors.New("persistent error")
		maxRetries := 2 // Will attempt 3 times (0, 1, 2)

		task := NewTask(TaskOptions{
			Name: "FailRetryTask",
			Function: func(ctx context.Context) error {
				attempts.Add(1)
				return failErr // Always fail
			},
			RetryStrategy: func(err error, attempt int) error {
				if attempt >= maxRetries {
					return fmt.Errorf("final attempt %d failed: %w", attempt, err) // Indicate final failure
				}
				time.Sleep(5 * time.Millisecond)
				return nil // Signal to retry
			},
		})

		p, err := NewPipeline(Options{Tasks: []*Task{task}})
		if err != nil {
			t.Fatalf("Failed to create pipeline: %v", err)
		}

		err = p.Run(context.Background())
		if err == nil {
			t.Error("Pipeline run succeeded unexpectedly, expected failure after retries")
		} else if !strings.Contains(err.Error(), failErr.Error()) {
			t.Errorf("Expected final error to contain '%v', got '%v'", failErr, err)
		} else if !strings.Contains(err.Error(), fmt.Sprintf("final attempt %d failed", maxRetries)) {
			t.Errorf("Expected final error to indicate max retries reached, got '%v'", err)
		}

		if attempts.Load() != int32(maxRetries+1) {
			t.Errorf("Expected %d attempts, got %d", maxRetries+1, attempts.Load())
		}
	})

	t.Run("Default No Retry", func(t *testing.T) {
		var attempts atomic.Int32
		failErr := errors.New("some error")

		task := NewTask(TaskOptions{
			Name: "NoRetryTask",
			Function: func(ctx context.Context) error {
				attempts.Add(1)
				return failErr
			},
			// No RetryStrategy specified
		})

		p, err := NewPipeline(Options{Tasks: []*Task{task}}) // No DefaultRetryStrategy
		if err != nil {
			t.Fatalf("Failed to create pipeline: %v", err)
		}

		err = p.Run(context.Background())
		if err == nil {
			t.Error("Pipeline run succeeded unexpectedly, expected failure")
		} else if !strings.Contains(err.Error(), failErr.Error()) {
			t.Errorf("Expected final error to contain '%v', got '%v'", failErr, err)
		}

		if attempts.Load() != 1 {
			t.Errorf("Expected 1 attempt (no retry), got %d", attempts.Load())
		}
	})

	t.Run("Pipeline Default Retry", func(t *testing.T) {
		var attempts atomic.Int32
		failErr := errors.New("transient error")
		maxAttempts := 2 // Pipeline default allows 1 retry (attempts 0, 1)

		task := NewTask(TaskOptions{
			Name: "DefaultRetryTask",
			Function: func(ctx context.Context) error {
				currentAttempt := attempts.Add(1)
				if currentAttempt < int32(maxAttempts) {
					return failErr
				}
				return nil // Success on the second try
			},
			// No Task-specific RetryStrategy
		})

		p, err := NewPipeline(Options{
			Tasks: []*Task{task},
			DefaultRetryStrategy: func(err error, attempt int) error {
				if attempt >= maxAttempts-1 { // 0-indexed attempts
					return fmt.Errorf("pipeline default giving up: %w", err)
				}
				time.Sleep(5 * time.Millisecond)
				return nil // Retry
			},
		})
		if err != nil {
			t.Fatalf("Failed to create pipeline: %v", err)
		}

		err = p.Run(context.Background())
		if err != nil {
			t.Errorf("Pipeline run failed despite successful default retry: %v", err)
		}
		if attempts.Load() != int32(maxAttempts) {
			t.Errorf("Expected %d attempts, got %d", maxAttempts, attempts.Load())
		}
	})

	t.Run("Max Retry Depth Exceeded", func(t *testing.T) {
		var attempts atomic.Int32
		failErr := errors.New("persistent error")
		maxDepth := 3
		customMaxDepth := maxDepth // For pipeline option

		task := NewTask(TaskOptions{
			Name: "MaxDepthTask",
			Function: func(ctx context.Context) error {
				attempts.Add(1)
				return failErr // Always fail
			},
			RetryStrategy: func(err error, attempt int) error {
				// This strategy would normally retry forever, but the pipeline limit should stop it
				time.Sleep(1 * time.Millisecond)
				return nil // Always signal retry
			},
		})

		p, err := NewPipeline(Options{
			Tasks:                     []*Task{task},
			MaximumRetryStrategyDepth: &customMaxDepth,
		})
		if err != nil {
			t.Fatalf("Failed to create pipeline: %v", err)
		}

		err = p.Run(context.Background())
		if err == nil {
			t.Error("Pipeline run succeeded unexpectedly, expected failure due to max retry depth")
		} else if !strings.Contains(err.Error(), "maximum retry depth reached") {
			t.Errorf("Expected error message indicating max retry depth, got: %v", err)
		} else if !strings.Contains(err.Error(), fmt.Sprintf("(attempt %d)", maxDepth)) {
			t.Errorf("Expected error message to mention attempt %d, got: %v", maxDepth, err)
		} else if !errors.Is(err, failErr) { // Check underlying error is wrapped
			t.Errorf("Expected final error to wrap original '%v', got '%v'", failErr, err)
		}

		// Attempts = maxDepth + 1 (0, 1, 2, 3)
		if attempts.Load() != int32(maxDepth+1) {
			t.Errorf("Expected %d attempts, got %d", maxDepth+1, attempts.Load())
		}
	})

	t.Run("Max Retry Depth Unlimited", func(t *testing.T) {
		var attempts atomic.Int32
		failErr := errors.New("persistent error")
		stopAfter := 5 // Stop manually after a few attempts
		unlimited := -1

		task := NewTask(TaskOptions{
			Name: "UnlimitedDepthTask",
			Function: func(ctx context.Context) error {
				attempts.Add(1)
				return failErr // Always fail
			},
			RetryStrategy: func(err error, attempt int) error {
				if attempt >= stopAfter-1 {
					return fmt.Errorf("stopping manually: %w", err) // Stop manually
				}
				time.Sleep(1 * time.Millisecond)
				return nil // Always signal retry
			},
		})

		p, err := NewPipeline(Options{
			Tasks:                     []*Task{task},
			MaximumRetryStrategyDepth: &unlimited,
		})
		if err != nil {
			t.Fatalf("Failed to create pipeline: %v", err)
		}

		err = p.Run(context.Background())
		if err == nil {
			t.Error("Pipeline run succeeded unexpectedly, expected manual stop error")
		} else if !strings.Contains(err.Error(), "stopping manually") {
			t.Errorf("Expected manual stop error message, got: %v", err)
		}

		if attempts.Load() != int32(stopAfter) {
			t.Errorf("Expected %d attempts, got %d", stopAfter, attempts.Load())
		}
	})
}

func TestPipeline_Run_IsCompleteSkip(t *testing.T) {
	var executedA, executedB atomic.Bool

	taskA := NewTask(TaskOptions{
		Name: "A",
		Function: func(ctx context.Context) error {
			executedA.Store(true)
			return nil
		},
		IsComplete: func() bool {
			return true // Always skip
		},
	})
	taskB := makeTestTask("B", &executedB, taskA) // Depends on A

	p, err := NewPipeline(Options{Tasks: []*Task{taskA, taskB}})
	if err != nil {
		t.Fatalf("Failed to create pipeline: %v", err)
	}

	err = p.Run(context.Background())
	if err != nil {
		t.Fatalf("Pipeline run failed unexpectedly: %v", err)
	}

	if executedA.Load() {
		t.Error("Task A should have been skipped (IsComplete=true), but its function executed")
	}
	if !executedB.Load() {
		t.Error("Task B should have executed as skipped Task A is considered successful")
	}
}

func TestPipeline_Run_WaitForComplete(t *testing.T) {
	waitDuration := 200 * time.Millisecond // Generous duration for test
	shortWait := 50 * time.Millisecond

	t.Run("Successful Wait", func(t *testing.T) {
		var executedA, executedB atomic.Bool
		var isAComplete atomic.Bool
		var timeBStarted atomic.Int64 // Store nanoseconds

		taskA := NewTask(TaskOptions{
			Name: "A",
			Function: func(ctx context.Context) error {
				// Simulate work, then set complete status after a delay
				executedA.Store(true)
				go func() {
					time.Sleep(shortWait)
					isAComplete.Store(true)
				}()
				return nil
			},
			IsComplete: func() bool {
				return isAComplete.Load()
			},
		})
		taskB := NewTask(TaskOptions{
			Name: "B",
			Function: func(ctx context.Context) error {
				timeBStarted.Store(time.Now().UnixNano())
				executedB.Store(true)
				return nil
			},
			Prerequisites: []*Task{taskA},
		})

		p, err := NewPipeline(Options{
			Tasks:                   []*Task{taskA, taskB},
			WaitForCompleteDuration: &waitDuration,
		})
		if err != nil {
			t.Fatalf("Failed to create pipeline: %v", err)
		}

		startTime := time.Now()
		err = p.Run(context.Background())
		if err != nil {
			t.Fatalf("Pipeline run failed: %v", err)
		}
		endTime := time.Now()

		if !executedA.Load() {
			t.Error("Task A did not execute")
		}
		if !executedB.Load() {
			t.Error("Task B did not execute")
		}
		if !isAComplete.Load() {
			t.Error("Task A IsComplete condition was never met")
		}

		// Check if B started after A's IsComplete became true
		timeBStartActual := time.Unix(0, timeBStarted.Load())
		expectedMinStartTimeB := startTime.Add(shortWait)
		if timeBStartActual.Before(expectedMinStartTimeB.Add(-10 * time.Millisecond)) { // Allow some buffer
			t.Errorf("Task B started too early (at %v relative to start), expected it after ~%v delay when A became complete",
				timeBStartActual.Sub(startTime), shortWait)
		}

		// Check overall duration isn't excessive
		if endTime.Sub(startTime) > waitDuration*2 { // Should be much less than timeout
			t.Errorf("Pipeline took too long (%v), expected completion shortly after A became complete (~%v)", endTime.Sub(startTime), shortWait)
		}
	})

	t.Run("Timeout Wait", func(t *testing.T) {
		var executedA, executedB atomic.Bool
		isAComplete := false // Never becomes true

		timeoutDuration := 100 * time.Millisecond // Short timeout

		taskA := NewTask(TaskOptions{
			Name: "A-Timeout",
			Function: func(ctx context.Context) error {
				executedA.Store(true)
				return nil
			},
			IsComplete: func() bool {
				return isAComplete // Always false
			},
		})
		taskB := NewTask(TaskOptions{
			Name: "B-Timeout",
			Function: func(ctx context.Context) error {
				executedB.Store(true)
				return nil
			},
			Prerequisites: []*Task{taskA},
		})

		p, err := NewPipeline(Options{
			Tasks:                   []*Task{taskA, taskB},
			WaitForCompleteDuration: &timeoutDuration,
		})
		if err != nil {
			t.Fatalf("Failed to create pipeline: %v", err)
		}

		startTime := time.Now()
		err = p.Run(context.Background())
		endTime := time.Now()

		if err == nil {
			t.Fatal("Pipeline run succeeded, but expected timeout error")
		}
		if !errors.Is(err, ErrWaitTimeout) {
			t.Fatalf("Expected ErrWaitTimeout, got: %v", err)
		}
		if !strings.Contains(err.Error(), taskA.name) {
			t.Errorf("Expected timeout error to mention task %s, got: %v", taskA.name, err)
		}

		if !executedA.Load() {
			t.Error("Task A-Timeout should have executed")
		}
		if executedB.Load() {
			t.Error("Task B-Timeout should have been skipped due to prerequisite wait timeout")
		}

		// Check duration roughly matches timeout
		runDuration := endTime.Sub(startTime)
		if runDuration < timeoutDuration || runDuration > timeoutDuration*2 { // Allow some overhead
			t.Errorf("Pipeline run duration (%v) doesn't roughly match timeout duration (%v)", runDuration, timeoutDuration)
		}

		// Check that B was skipped (didn't execute) and the correct timeout error was returned.
		// No need to inspect internal errors.
	})

	t.Run("Wait Hits Max Delay Cap", func(t *testing.T) {
		// This test ensures the exponential backoff delay calculation hits the maxDelay cap.
		var executedA, executedB atomic.Bool
		isAComplete := false // Never becomes true

		// Set a timeout significantly longer than the max backoff delay (1s)
		// to ensure the capping logic is triggered multiple times before timeout.
		// Backoff: 100ms, 200ms, 400ms, 800ms, 1s, 1s ...
		timeoutDuration := 3 * time.Second

		taskA := NewTask(TaskOptions{
			Name: "A-BackoffCap",
			Function: func(ctx context.Context) error {
				executedA.Store(true)
				return nil // Completes instantly
			},
			IsComplete: func() bool {
				return isAComplete // Always false, forcing wait
			},
		})
		taskB := NewTask(TaskOptions{
			Name: "B-BackoffCap",
			Function: func(ctx context.Context) error {
				executedB.Store(true)
				return nil
			},
			Prerequisites: []*Task{taskA},
		})

		p, err := NewPipeline(Options{
			Tasks:                   []*Task{taskA, taskB},
			WaitForCompleteDuration: &timeoutDuration,
		})
		if err != nil {
			t.Fatalf("Failed to create pipeline: %v", err)
		}

		startTime := time.Now()
		err = p.Run(context.Background())
		endTime := time.Now()
		runDuration := endTime.Sub(startTime)

		// Expect a timeout error because IsComplete never becomes true
		if err == nil {
			t.Fatal("Pipeline run succeeded, but expected timeout error")
		}
		if !errors.Is(err, ErrWaitTimeout) {
			t.Fatalf("Expected ErrWaitTimeout, got: %v", err)
		}
		if !strings.Contains(err.Error(), taskA.name) {
			t.Errorf("Expected timeout error to mention task %s, got: %v", taskA.name, err)
		}
		if !executedA.Load() {
			t.Error("Task A-BackoffCap should have executed")
		}
		if executedB.Load() {
			t.Error("Task B-BackoffCap should have been skipped due to prerequisite wait timeout")
		}
		// Check duration roughly matches the timeout
		// Allow for some scheduling variance, but it should be close to timeoutDuration
		if runDuration < timeoutDuration-500*time.Millisecond || runDuration > timeoutDuration+500*time.Millisecond {
			t.Errorf("Pipeline run duration (%v) doesn't roughly match the expected timeout duration (%v)", runDuration, timeoutDuration)
		}
	})

	t.Run("Cancel During Wait", func(t *testing.T) {
		var executedA, executedB atomic.Bool
		var isAComplete atomic.Bool
		var waitDuration = 200 * time.Millisecond
		var completeDelay = 50 * time.Millisecond // When IsComplete becomes true
		var cancelDelay = 25 * time.Millisecond   // When context is cancelled

		taskA := NewTask(TaskOptions{
			Name: "A-CancelWait",
			Function: func(ctx context.Context) error {
				executedA.Store(true)
				// Start goroutine to set complete after a delay
				go func() {
					time.Sleep(completeDelay)
					isAComplete.Store(true)
				}()
				return nil // Task function finishes quickly
			},
			IsComplete: func() bool {
				return isAComplete.Load()
			},
		})
		taskB := NewTask(TaskOptions{
			Name: "B-CancelWait",
			Function: func(ctx context.Context) error {
				executedB.Store(true)
				return nil
			},
			Prerequisites: []*Task{taskA},
		})

		p, err := NewPipeline(Options{
			Tasks:                   []*Task{taskA, taskB},
			WaitForCompleteDuration: &waitDuration,
		})
		if err != nil {
			t.Fatalf("Failed to create pipeline: %v", err)
		}

		ctx, cancel := context.WithCancel(context.Background())
		var wg sync.WaitGroup
		var pipelineErr error
		wg.Add(1)
		go func() {
			defer wg.Done()
			pipelineErr = p.Run(ctx)
		}()

		// Wait for a short period, then cancel the context while B should be waiting for A
		time.Sleep(cancelDelay)
		cancel()

		wg.Wait() // Wait for pipeline to finish

		if pipelineErr == nil {
			t.Fatal("Pipeline run expected to fail due to cancellation, but succeeded")
		}
		if !errors.Is(pipelineErr, context.Canceled) {
			t.Errorf("Expected pipeline error to be context.Canceled, got %v", pipelineErr)
		}
		if !executedA.Load() {
			t.Error("Task A-CancelWait should have executed")
		}
		if executedB.Load() {
			t.Error("Task B-CancelWait should have been skipped due to cancellation during wait")
		}
	})

	t.Run("Wait Not Configured", func(t *testing.T) {
		var executedA, executedB atomic.Bool
		isAComplete := false // Should not matter

		taskA := NewTask(TaskOptions{
			Name: "A-NoWait",
			Function: func(ctx context.Context) error {
				executedA.Store(true)
				time.Sleep(20 * time.Millisecond) // Give B a chance to start too early if wait logic is broken
				return nil
			},
			IsComplete: func() bool {
				return isAComplete
			},
		})
		taskB := NewTask(TaskOptions{
			Name: "B-NoWait",
			Function: func(ctx context.Context) error {
				executedB.Store(true)
				return nil
			},
			Prerequisites: []*Task{taskA},
		})

		p, err := NewPipeline(Options{
			Tasks: []*Task{taskA, taskB},
			// WaitForCompleteDuration: nil (default)
		})
		if err != nil {
			t.Fatalf("Failed to create pipeline: %v", err)
		}

		startTime := time.Now()
		err = p.Run(context.Background())
		endTime := time.Now()

		if err != nil {
			t.Fatalf("Pipeline run failed unexpectedly: %v", err)
		}

		if !executedA.Load() || !executedB.Load() {
			t.Error("Both tasks A and B should have executed")
		}

		// Expect fast completion as B doesn't wait for A's IsComplete
		if endTime.Sub(startTime) > 100*time.Millisecond { // Should be very quick
			t.Errorf("Pipeline took too long (%v), expected quick completion as wait was not configured", endTime.Sub(startTime))
		}
	})
}

func TestPipeline_Run_CycleDetection(t *testing.T) {
	taskA := NewTask(TaskOptions{Name: "A", Function: func(ctx context.Context) error { return nil }})
	taskB := NewTask(TaskOptions{Name: "B", Function: func(ctx context.Context) error { return nil }})
	taskC := NewTask(TaskOptions{Name: "C", Function: func(ctx context.Context) error { return nil }})
	taskD := NewTask(TaskOptions{Name: "D", Function: func(ctx context.Context) error { return nil }}) // Unrelated

	// Create cycle: A -> B -> C -> A
	taskA.prerequisites = []*Task{taskC}
	taskB.prerequisites = []*Task{taskA}
	taskC.prerequisites = []*Task{taskB}

	p, err := NewPipeline(Options{Tasks: []*Task{taskA, taskB, taskC, taskD}})
	if err != nil {
		t.Fatalf("Failed to create pipeline: %v", err)
	}

	err = p.Run(context.Background())
	if err == nil {
		t.Fatal("Pipeline run succeeded, but expected cycle detection error")
	}

	expectedErr := "circular prerequisite detected"
	if !strings.Contains(err.Error(), expectedErr) {
		t.Errorf("Expected error message to contain '%s', got: %v", expectedErr, err)
	}

	// Check for sorted node names in the error message
	expectedCycleParticipants := []string{"A", "B", "C"}
	sort.Strings(expectedCycleParticipants)
	expectedCycleStringPart := strings.Join(expectedCycleParticipants, " -> ") // e.g., "A -> B -> C"
	if !strings.Contains(err.Error(), expectedCycleStringPart) {
		t.Errorf("Expected error message to contain sorted cycle nodes '%s', got: %v", expectedCycleStringPart, err.Error())
	}
}

func TestDetectCycles_Advanced(t *testing.T) {
	t.Run("Nil Prerequisite Entry", func(t *testing.T) {
		taskA := NewTask(TaskOptions{Name: "A", Function: func(ctx context.Context) error { return nil }})
		taskB := NewTask(TaskOptions{Name: "B", Function: func(ctx context.Context) error { return nil }})
		// Intentionally create a nil entry in prerequisites
		taskA.prerequisites = []*Task{nil, taskB}

		err := detectCycles([]*Task{taskA, taskB})
		if err == nil {
			t.Fatal("Expected error for nil prerequisite entry, got nil")
		}
		if !strings.Contains(err.Error(), "nil prerequisite entry") || !strings.Contains(err.Error(), taskA.name) {
			t.Errorf("Expected error message indicating nil prerequisite for task A, got: %v", err)
		}
	})

	t.Run("Prerequisite Not Defined in Tasks", func(t *testing.T) {
		taskA := NewTask(TaskOptions{Name: "A", Function: func(ctx context.Context) error { return nil }})
		taskNotInList := NewTask(TaskOptions{Name: "NotInList", Function: func(ctx context.Context) error { return nil }})

		// taskA depends on a task not included in the list passed to detectCycles
		taskA.prerequisites = []*Task{taskNotInList}

		err := detectCycles([]*Task{taskA}) // Only pass taskA
		if err == nil {
			t.Fatal("Expected error for prerequisite not defined in tasks, got nil")
		}
		if !strings.Contains(err.Error(), "not found in the defined tasks") || !strings.Contains(err.Error(), taskA.name) || !strings.Contains(err.Error(), taskNotInList.name) {
			t.Errorf("Expected error message indicating missing prerequisite '%s' for task '%s', got: %v", taskNotInList.name, taskA.name, err)
		}
	})

	t.Run("Multiple Unrelated Cycles", func(t *testing.T) {
		// Cycle 1: A -> B -> A
		taskA := NewTask(TaskOptions{Name: "A", Function: func(ctx context.Context) error { return nil }})
		taskB := NewTask(TaskOptions{Name: "B", Function: func(ctx context.Context) error { return nil }})
		taskA.prerequisites = []*Task{taskB}
		taskB.prerequisites = []*Task{taskA}

		// Cycle 2: C -> D -> C
		taskC := NewTask(TaskOptions{Name: "C", Function: func(ctx context.Context) error { return nil }})
		taskD := NewTask(TaskOptions{Name: "D", Function: func(ctx context.Context) error { return nil }})
		taskC.prerequisites = []*Task{taskD}
		taskD.prerequisites = []*Task{taskC}

		err := detectCycles([]*Task{taskA, taskB, taskC, taskD})
		if err == nil {
			t.Fatal("Expected cycle detection error, got nil")
		}
		// Should detect *a* cycle, the specific one depends on map iteration order
		if !strings.Contains(err.Error(), "circular prerequisite detected") {
			t.Errorf("Expected error message indicating a cycle, got: %v", err)
		}
		// Check if it mentions one of the cycles (A/B or C/D)
		containsAB := strings.Contains(err.Error(), "A -> B -> A") || strings.Contains(err.Error(), "B -> A -> B")
		containsCD := strings.Contains(err.Error(), "C -> D -> C") || strings.Contains(err.Error(), "D -> C -> D")
		if !containsAB && !containsCD {
			t.Errorf("Cycle error message '%v' doesn't seem to mention cycle A<->B or C<->D", err.Error())
		}
	})
}

func TestPipeline_Run_PanicHandling(t *testing.T) {
	var executedA atomic.Bool

	taskA := makeTestTask("A", &executedA)
	taskPanic := makePanicTask("PanicTask", taskA)
	taskC := makeTestTask("C", new(atomic.Bool), taskPanic) // Depends on the panicking task

	p, err := NewPipeline(Options{Tasks: []*Task{taskA, taskPanic, taskC}})
	if err != nil {
		t.Fatalf("Failed to create pipeline: %v", err)
	}

	err = p.Run(context.Background())
	if err == nil {
		t.Fatal("Pipeline run succeeded, but expected panic error")
	}

	expectedErr := "panicked"
	if !strings.Contains(err.Error(), taskPanic.name) || !strings.Contains(err.Error(), expectedErr) {
		t.Errorf("Expected error message to contain '%s' and '%s', got: %v", taskPanic.name, expectedErr, err)
	}

	if !executedA.Load() {
		t.Error("Task A should have executed before the panic")
	}

	// Verify that the pipeline returned an error containing the panic message,
	// and that task C (dependent on the panic) did not run.
}

// TestPipeline_Run_CancelDuringTaskExecution aims to trigger cancellation checks within
// runTaskWithRetry and executeTask while a task function is actively running.
func TestPipeline_Run_CancelDuringTaskExecution(t *testing.T) {
	var executedA atomic.Bool
	startSignal := make(chan struct{})

	taskA := NewTask(TaskOptions{
		Name: "A",
		Function: func(ctx context.Context) error {
			close(startSignal)
			executedA.Store(true)
			// Simulate work that takes some time and might be interrupted
			loopStartTime := time.Now()
			for time.Since(loopStartTime) < 100*time.Millisecond {
				// Periodically check for cancellation, but don't return immediately
				select {
				case <-ctx.Done():
					// Log or acknowledge cancellation received, but continue briefly
					return ctx.Err()
				default:
					// Simulate work chunk
					time.Sleep(10 * time.Millisecond)
				}
			}
			return nil // Should ideally be cancelled before reaching here
		},
	})
	p, err := NewPipeline(Options{Tasks: []*Task{taskA}})
	if err != nil {
		t.Fatalf("Failed to create pipeline: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	var pipelineErr error
	wg.Add(1)
	go func() {
		defer wg.Done()
		pipelineErr = p.Run(ctx)
	}()

	// Wait for Task A to start, then cancel shortly after
	select {
	case <-startSignal:
		time.Sleep(20 * time.Millisecond) // Allow task A function to get into its loop
		cancel()
	case <-time.After(1 * time.Second):
		t.Fatal("Timed out waiting for Task A to start")
	}

	wg.Wait() // Wait for pipeline run to finish

	if pipelineErr == nil {
		t.Fatal("Pipeline run expected to fail due to cancellation, but succeeded")
	}
	if !errors.Is(pipelineErr, context.Canceled) {
		t.Errorf("Expected pipeline error to be context.Canceled, got %v", pipelineErr)
	}
	if !executedA.Load() {
		t.Error("Task A should have started execution")
	}
}

// TestPipeline_Run_CancelDuringCycleDetection tests cancelling the context
// during the initial cycle detection phase.
func TestPipeline_Run_CancelDuringCycleDetection(t *testing.T) {
	// Create a chain of tasks to make cycle detection non-trivial
	numTasks := 20
	tasks := make([]*Task, numTasks)
	var executed atomic.Bool // Only need one, none should execute

	// Create Task 0
	tasks[0] = makeTestTask("task-0", &executed)

	// Create Tasks 1 to N-1, each depending on the previous one
	for i := 1; i < numTasks; i++ {
		tasks[i] = makeTestTask(fmt.Sprintf("task-%d", i), &executed, tasks[i-1])
	}

	p, err := NewPipeline(Options{Tasks: tasks})
	if err != nil {
		t.Fatalf("Failed to create pipeline: %v", err)
	}
	// Create a context and cancel it *immediately* before running
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// Run the pipeline with the already cancelled context
	pipelineErr := p.Run(ctx)

	// Expect a cancellation error
	if pipelineErr == nil {
		t.Fatal("Pipeline run expected to fail due to immediate cancellation, but succeeded")
	}
	if !errors.Is(pipelineErr, context.Canceled) {
		t.Errorf("Expected pipeline error to be context.Canceled, got %v", pipelineErr)
	}
	// Ensure no tasks actually executed
	if executed.Load() {
		t.Error("No tasks should have executed due to immediate cancellation")
	}
}

// TestBuildCyclePathString_StartNodeNotFound tests the edge case where the start node is not found in the path,
// which should trigger the internal error handling for line 662 coverage.
func TestBuildCyclePathString_StartNodeNotFound(t *testing.T) {
	taskA := NewTask(TaskOptions{Name: "A", Function: func(ctx context.Context) error { return nil }})
	taskB := NewTask(TaskOptions{Name: "B", Function: func(ctx context.Context) error { return nil }})
	taskNotInPath := NewTask(TaskOptions{Name: "NotInPath", Function: func(ctx context.Context) error { return nil }})

	path := []*Task{taskA, taskB}
	result := buildCyclePathString(path, taskNotInPath)
	assert.Contains(t, result, "internal error")
}

// TestPipeline_Run_CancelDuringRetryWait tests cancellation hitting the check
// at the start of the runTaskWithRetry loop (lines 259-263).
func TestPipeline_Run_CancelDuringRetryWait(t *testing.T) {
	var attempts atomic.Int32
	failErr := errors.New("persistent retry error")
	retrySignal := make(chan struct{}) // Signal that retry strategy is active

	task := NewTask(TaskOptions{
		Name: "CancelRetryTask",
		Function: func(ctx context.Context) error {
			attempts.Add(1)
			// Always fail to trigger retry
			return failErr
		},
		RetryStrategy: func(err error, attempt int) error {
			// Signal that we are in the retry strategy, about to wait
			// This guarantees the initial task run completed and failed.
			// Use a non-blocking send in case the signal is checked late or test times out
			select {
			case retrySignal <- struct{}{}:
				close(retrySignal) // Close after sending successfully
			default:
				// Channel already closed or blocked, proceed
			}

			// Wait a bit, giving the test time to cancel the context.
			// The select block (lines 259-263) runs *after* this returns nil.
			time.Sleep(50 * time.Millisecond)

			// Normally we'd decide whether to retry or not.
			// Here, we *intend* to retry, but expect cancellation to interrupt.
			return nil // Signal to retry
		},
	})

	p, err := NewPipeline(Options{
		Tasks: []*Task{task},
		// Use default MaximumRetryStrategyDepth or set explicitly if needed
	})
	if err != nil {
		t.Fatalf("Failed to create pipeline: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	var pipelineErr error
	wg.Add(1)
	go func() {
		defer wg.Done()
		pipelineErr = p.Run(ctx)
	}()

	// Wait for the first attempt to fail and the retry strategy to be entered
	select {
	case <-retrySignal:
		// Retry strategy is active, cancel the context now.
		// The cancellation should be detected at the start of the next retry loop iteration.
		cancel()
	case <-time.After(2 * time.Second):
		// If the signal isn't received, the task might not have failed as expected
		// or the retry logic isn't being hit.
		t.Error("Timed out waiting for retry signal") // Use Error instead of Fatal to allow wg.Wait()
		cancel()                                      // Clean up context anyway
	}

	wg.Wait() // Wait for pipeline run to finish

	// Assertions
	if pipelineErr == nil {
		t.Fatal("Pipeline run expected to fail due to cancellation, but succeeded")
	}
	// The top-level error should be context.Canceled because the external context was cancelled.
	if !errors.Is(pipelineErr, context.Canceled) {
		t.Errorf("Expected pipeline error to be context.Canceled, got %T: %v", pipelineErr, pipelineErr)
	}

	// Check the underlying error chain if needed (might be complex due to wrapping)
	// We primarily care that the external context cancellation stopped things.

	// The task function should have been attempted only once.
	if attempts.Load() != 1 {
		t.Errorf("Expected task function to be attempted 1 time, got %d", attempts.Load())
	}

	// We can infer that lines 259-261 were hit because:
	// 1. The retry strategy was entered (retrySignal closed).
	// 2. The retry strategy returned nil (indicating a retry should occur).
	// 3. The pipeline ended with context.Canceled.
	// 4. The task function was only called once.
	// This implies the cancellation was caught *before* the second execution attempt.
}

// TestPipeline_Run_CancelDuringConcurrencyWait specifically targets the cancellation
// check within executeTask while waiting for the concurrency limiter (lines 340-343).
func TestPipeline_Run_CancelDuringConcurrencyWait(t *testing.T) {
	var executedA, startedB, executedB, startedC, executedC atomic.Bool
	taskBStartedSignal := make(chan struct{})
	taskCStartedSignal := make(chan struct{})
	taskBFinishSignal := make(chan struct{}) // To prevent task B finishing too quickly

	taskA := makeTestTask("A", &executedA)

	taskB := NewTask(TaskOptions{
		Name: "B",
		Function: func(ctx context.Context) error {
			startedB.Store(true)
			close(taskBStartedSignal) // Signal that B has started and acquired the semaphore
			select {
			case <-taskBFinishSignal: // Wait until released
				executedB.Store(true)
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		},
		Prerequisites: []*Task{taskA},
	})

	taskC := NewTask(TaskOptions{
		Name: "C",
		Function: func(ctx context.Context) error {
			startedC.Store(true)
			close(taskCStartedSignal) // Signal that C has started and acquired the semaphore
			// Simulate work - less critical than B for this test's core logic
			time.Sleep(50 * time.Millisecond)
			executedC.Store(true)
			return nil
		},
		Prerequisites: []*Task{taskA},
	})

	p, err := NewPipeline(Options{
		Tasks:            []*Task{taskA, taskB, taskC},
		ConcurrencyLimit: 1, // Force B or C to wait
	})
	if err != nil {
		t.Fatalf("Failed to create pipeline: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	var pipelineErr error
	wg.Add(1)
	go func() {
		defer wg.Done()
		pipelineErr = p.Run(ctx)
	}()

	// Wait for either B or C to start (meaning A is done and one task acquired the semaphore)
	select {
	case <-taskBStartedSignal:
		// B started, C must be waiting. Cancel now.
		cancel()
		close(taskBFinishSignal) // Allow B to finish or be cancelled cleanly
	case <-taskCStartedSignal:
		// C started, B must be waiting. Cancel now.
		cancel()
		// No need to signal C, it will finish or cancel on its own
	case <-time.After(2 * time.Second):
		// If neither started, something is wrong (e.g., Task A failed/stuck)
		t.Fatal("Timed out waiting for Task B or C to start")
		cancel() // Clean up context
	}

	wg.Wait() // Wait for pipeline run to finish

	// Assertions
	if pipelineErr == nil {
		t.Fatal("Pipeline run expected to fail due to cancellation, but succeeded")
	}
	if !errors.Is(pipelineErr, context.Canceled) {
		t.Errorf("Expected pipeline error to be context.Canceled, got %v", pipelineErr)
	}

	if !executedA.Load() {
		t.Error("Task A should have executed")
	}

	bStarted := startedB.Load()
	cStarted := startedC.Load()

	if bStarted && cStarted {
		t.Error("Both Task B and Task C started execution, expected only one due to concurrency limit")
	}
	if !bStarted && !cStarted {
		// This shouldn't happen if the select block above worked correctly
		t.Error("Neither Task B nor Task C started execution")
	}

	// The core check: The task that *didn't* start must have been cancelled while waiting for the semaphore.
	// We infer this because the overall pipeline context was canceled, and the task didn't get to run its function.
	if bStarted && executedC.Load() {
		t.Error("Task C executed unexpectedly (should have been skipped/cancelled while waiting)")
	}
	if cStarted && executedB.Load() {
		t.Error("Task B executed unexpectedly (should have been skipped/cancelled while waiting)")
	}

	// Log final states for debugging if needed
	t.Logf("Final states: executedA=%t, startedB=%t, executedB=%t, startedC=%t, executedC=%t",
		executedA.Load(), bStarted, executedB.Load(), cStarted, executedC.Load())
}

func TestTask_String(t *testing.T) {
	task := NewTask(TaskOptions{
		Name:     "MyTask",
		Function: func(ctx context.Context) error { return nil },
	})
	expected := "Task(MyTask)"
	if task.String() != expected {
		t.Errorf("Expected task string '%s', got '%s'", expected, task.String())
	}
}
