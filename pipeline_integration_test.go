package idempipe

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPipeline_FileOperationsIntegration(t *testing.T) {
	tempDir := t.TempDir()
	testFilePath := tempDir + "/testfile.txt"
	initialContent := "Hello, Pipeline!"
	readContent := ""
	var readMu sync.Mutex

	// Task to write the file
	writeFileTask, errWrite := NewTask(TaskOptions{
		Name:          "WriteFile",
		Prerequisites: nil,
		Function: func(ctx context.Context) error {
			err := os.WriteFile(testFilePath, []byte(initialContent), 0644)
			if err != nil {
				return fmt.Errorf("failed to write file: %w", err)
			}
			return nil
		},
	})
	require.NoError(t, errWrite)

	// Task to read the file
	readFileTask, errRead := NewTask(TaskOptions{
		Name:          "ReadFile",
		Prerequisites: []*Task{writeFileTask},
		Function: func(ctx context.Context) error {
			contentBytes, err := os.ReadFile(testFilePath)
			if err != nil {
				return fmt.Errorf("failed to read file: %w", err)
			}
			readMu.Lock()
			readContent = string(contentBytes)
			readMu.Unlock()
			if readContent != initialContent {
				return fmt.Errorf("read content mismatch: expected %q, got %q", initialContent, readContent)
			}
			return nil
		},
	})
	require.NoError(t, errRead)

	// Task to delete the file
	deleteFileTask, errDelete := NewTask(TaskOptions{
		Name:          "DeleteFile",
		Prerequisites: []*Task{readFileTask},
		Function: func(ctx context.Context) error {
			err := os.Remove(testFilePath)
			if err != nil {
				return fmt.Errorf("failed to delete file: %w", err)
			}
			return nil
		},
	})
	require.NoError(t, errDelete)

	logger := &testLogger{}
	pipeline, err := NewPipeline(Options{
		Name:   "FileOpsTest",
		Tasks:  []*Task{writeFileTask, readFileTask, deleteFileTask},
		Logger: logger.Log,
	})
	require.NoError(t, err)

	ctx := context.Background()
	pipelineErr := pipeline.Run(ctx)

	assert.NoError(t, pipelineErr, "Pipeline should run successfully")

	logOutput := logger.String()
	assert.Contains(t, logOutput, "Task(WriteFile): Completed")
	assert.Contains(t, logOutput, "Task(ReadFile): Completed")
	assert.Contains(t, logOutput, "Task(DeleteFile): Completed")
	assert.Contains(t, logOutput, "All tasks completed successfully")

	readMu.Lock()
	assert.Equal(t, initialContent, readContent, "Content read should match content written")
	readMu.Unlock()

	// Verify the file is actually deleted
	_, err = os.Stat(testFilePath)
	assert.True(t, os.IsNotExist(err), "Test file should have been deleted")
}

func TestPipeline_ComplexPrerequisiteIntegration(t *testing.T) {
	delay := 10 * time.Millisecond

	// Use helper function (defined in pipeline_test.go) which now requires t
	taskA := successfulTask(t, "A", delay)
	taskB := successfulTask(t, "B", delay, taskA)
	taskC := successfulTask(t, "C", delay, taskA)
	taskD := successfulTask(t, "D", delay, taskB, taskC) // Depends on both B and C

	logger := &testLogger{}
	pipeline, err := NewPipeline(Options{
		Name:   "ComplexDepTest",
		Tasks:  []*Task{taskA, taskB, taskC, taskD},
		Logger: logger.Log,
	})
	require.NoError(t, err)

	ctx := context.Background()
	start := time.Now()
	pipelineErr := pipeline.Run(ctx)
	duration := time.Since(start)

	assert.NoError(t, pipelineErr, "Pipeline should run successfully")

	logOutput := logger.String()
	fmt.Println(logOutput) // Print logs for debugging if needed

	// Basic completion checks
	assert.Contains(t, logOutput, "Task(A): Completed")
	assert.Contains(t, logOutput, "Task(B): Completed")
	assert.Contains(t, logOutput, "Task(C): Completed")
	assert.Contains(t, logOutput, "Task(D): Completed")
	assert.Contains(t, logOutput, "All tasks completed successfully")

	// Check execution order based on logs
	// Find indices of key log messages
	aCompletedIdx := strings.Index(logOutput, "Task(A): Completed")
	bStartingIdx := strings.Index(logOutput, "Task(B): Starting...")
	cStartingIdx := strings.Index(logOutput, "Task(C): Starting...")
	bCompletedIdx := strings.Index(logOutput, "Task(B): Completed")
	cCompletedIdx := strings.Index(logOutput, "Task(C): Completed")
	dStartingIdx := strings.Index(logOutput, "Task(D): Starting...")

	assert.NotEqual(t, -1, aCompletedIdx, "Log should contain 'Task(A): Completed'")
	assert.NotEqual(t, -1, bStartingIdx, "Log should contain 'Task(B): Starting...'")
	assert.NotEqual(t, -1, cStartingIdx, "Log should contain 'Task(C): Starting...'")
	assert.NotEqual(t, -1, bCompletedIdx, "Log should contain 'Task(B): Completed'")
	assert.NotEqual(t, -1, cCompletedIdx, "Log should contain 'Task(C): Completed'")
	assert.NotEqual(t, -1, dStartingIdx, "Log should contain 'Task(D): Starting...'")

	// Verify B and C start after A completes
	assert.Greater(t, bStartingIdx, aCompletedIdx, "B should start after A completes")
	assert.Greater(t, cStartingIdx, aCompletedIdx, "C should start after A completes")

	// Verify D starts after both B and C complete
	assert.Greater(t, dStartingIdx, bCompletedIdx, "D should start after B completes")
	assert.Greater(t, dStartingIdx, cCompletedIdx, "D should start after C completes")

	// Optional: Check approximate duration - should be around 3 * delay because A runs, then B & C in parallel, then D
	minDuration := 3 * delay
	maxDuration := minDuration + 50*time.Millisecond // Allow some overhead
	assert.GreaterOrEqual(t, duration, minDuration, "Pipeline duration should be at least 3 delays")
	assert.LessOrEqual(t, duration, maxDuration, "Pipeline duration should be roughly 3 delays + overhead")
}
