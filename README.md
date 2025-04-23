# Idempipe
[![Go Reference](https://pkg.go.dev/badge/github.com/jjgmckenzie/idempipe.svg)](https://pkg.go.dev/github.com/jjgmckenzie/idempipe) [![Go Report Card](https://goreportcard.com/badge/github.com/jjgmckenzie/idempipe)](https://goreportcard.com/report/github.com/jjgmckenzie/idempipe) [![Go Coverage](https://github.com/jjgmckenzie/idempipe/wiki/coverage.svg)](https://raw.githack.com/wiki/jjgmckenzie/idempipe/coverage.html)

Simple, concurrent, idempotent DAG pipelines in Go

## About 

An extremely simple Go library for defining and executing static pipelines of idempotent tasks based on a Directed Acyclic Graph (DAG). Instead of using DAG terminology, more intuitive terms like `Task` (for nodes) and `Prerequisites` (for edges/links) are used.  It handles prerequisites, concurrency, error propagation, retry strategies, context cancellation, and cycle detection.

This was created for personal use because alternatives seemed overengineered. A simple idempotent data processing pipeline was required across multiple client projects, specifically one supporting concurrency and error propagation *without* cancelling other independent branches upon failure (though idiomatic pipeline-wide cancellation via `context.Context` remains possible)

Additionally, both pipelines and tasks are static once created and do not change. Prerequisites are not dynamically determined. Instead of dynamic prerequisites, consider using a chain-of-responsibility pattern. Tasks are designed to be idempotent, allowing the pipeline to be safely restarted and resume execution by checking completion status at the beginning of each task.

Data is also never passed between tasks; and each task is entirely atomic. State management (e.g., saving/loading results between tasks) is the responsibility of the application. If you require state management, consider using this [modern, typesafe pipeline orchestration library](https://github.com/Synoptiq/go-fluxus) instead.

### Why Idempotency?

Idempotency ensures tasks produce the same result regardless of execution count. This makes pipelines:

- **Resilient**: Safely restart or resume after crashes or halts.
- **Retry-friendly**: Failed tasks can be retried without side effects.
- **Predictable & Debuggable**: Outcome is deterministic; tasks can be re-run in isolation.
- **Extensible**: New tasks can be added anywhere in the pipeline without unintended side effects or interference with completed or independent tasks

This approach simplifies implementation by reducing the need for complex transaction management.

### Features

-   **Static Task Definition**: Define tasks and their prerequisites upfront using `idempipe.NewTask`.
-   **DAG Execution**: Automatically determines execution order based on prerequisites.
-   **Concurrency**: Executes independent task branches in parallel.
-   **Granular Error Propagation**: Failures only halt downstream dependent tasks; independent branches continue.
-   **Flexible Retry Strategies**: Define custom retry logic per-task (`idempipe.TaskOptions`) or set a default (`idempipe.Options`).
-   **Panic Handling**: Recovers from task panics, treating them as errors for retries/failure.
-   **Context Cancellation**: Supports standard Go `context.Context` for graceful pipeline shutdown.
-   **DAG Validation**: Detects cycles and missing prerequisites at initialization.
-   **Pluggable Logging**: Provide a custom logging function (`idempipe.Options`).

### How it Works

1. **Define Tasks**  
   Each `idempipe.Task` represents an atomic unit of work. Use the `idempipe.NewTask` constructor with `idempipe.TaskOptions` to create tasks, providing:
   - A name for the task
   - The function to execute
   - A slice of prerequisites (other tasks that must complete first)
   - `RetryStrategy` (Optional): A function ``func(err error, attempts int) error`` that decides whether a failed task should be retried.
   - Returning ``nil`` indicates retry should proceed; any non-nil error stops retries and is surfaced
   - `ShouldSkip` (Optional): A function `func() bool`. If provided and returns `true`, the task's function will not be executed. The task is logged as skipped and considered successfully completed for prerequisite checking by dependent tasks.

   `NewTask` returns the Task pointer and an error.

2. **Create Pipeline**  
   Use `idempipe.NewPipeline` with `idempipe.Options` to configure the pipeline. You'll need to provide all defined tasks (created via `NewTask`) and can optionally specify logging, retry strategies, and concurrency limits.

3. **Run Pipeline**  
   Call the `Run` method on the pipeline instance with a `context.Context`. The library:
   - Builds the DAG
   - Checks for cycles (will fail if detected)
   - Executes tasks concurrently based on prerequisites

4. **Error Handling & Retries**  
   If a task function returns an error or panics, the pipeline:
   - Consults the task's specific `RetryStrategy` or the pipeline's `DefaultRetryStrategy`
   - Determines if the task should be retried, potentially after a delay
   - Marks the task as failed if retry strategy decides not to retry or retries are exhausted
   - Skips any dependent tasks (direct or indirect)
   - Continues executing independent tasks
   
   The `Run` method returns an aggregated error if any tasks ultimately fail.

5. **Cancellation**  
   If the provided `context.Context` is cancelled:
   - Ongoing tasks receive the cancellation signal
   - All pending tasks are skipped
   - `Run` returns the context's error
   
   Task functions are responsible for handling cancellation signals within their execution.


## Usage

### Installation

```bash
go get github.com/jjgmckenzie/idempipe
```

### Example 

Define your pipeline tasks and their prerequisites, then create and run the pipeline.

```go
package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jjgmckenzie/idempipe"
)

func main() {
	// Define Tasks using NewTask constructor
	taskA, errA := idempipe.NewTask(idempipe.TaskOptions{Name: "A", Function: func(ctx context.Context) error {
		fmt.Println("[EXAMPLE] Executing Task A...")
		time.Sleep(100 * time.Millisecond)
		fmt.Println("[EXAMPLE] Task A Completed.")
		return nil
	}})
	if errA != nil {
		fmt.Printf("[EXAMPLE] Error creating task A: %v\n", errA)
		return
	}

	// Task B with a simple retry strategy
	var taskBFailedOnce bool // Flag to make Task B fail only the first time
	taskB, errB := idempipe.NewTask(idempipe.TaskOptions{
		Name:         "B",
		Prerequisites: []*idempipe.Task{taskA},
		Function: func(ctx context.Context) error {
			fmt.Println("[EXAMPLE] Executing Task B...")
			time.Sleep(50 * time.Millisecond)
			if !taskBFailedOnce {
				taskBFailedOnce = true // Set flag after first failure
				fmt.Println("[EXAMPLE] Task B is failing once intentionally.")
				return errors.New("simulated temporary failure")
			}
			fmt.Println("[EXAMPLE] Task B Completed successfully on retry.")
			return nil
		},
		RetryStrategy: func(err error, attempt int) error {
			// Retry up to 1 time (total 2 attempts) for any error
			if attempt < 1 {
				fmt.Printf("[EXAMPLE] RetryStrategy for Task B: Retrying after error '%v' (attempt %d).\n", err, attempt+1)
				// No backoff needed for this simple example
				return nil // Signal that retry should occur
			}
			fmt.Printf("[EXAMPLE] RetryStrategy for Task B: Max retries reached after error '%v' (attempt %d).\n", err, attempt+1)
			return err // Signal that retry should stop
		},
	})
	if errB != nil {
		fmt.Printf("[EXAMPLE] Error creating task B: %v\n", errB)
		return
	}

	taskC, errC := idempipe.NewTask(idempipe.TaskOptions{Name: "C", Prerequisites: []*idempipe.Task{taskA}, Function: func(ctx context.Context) error {
		fmt.Println("[EXAMPLE] Executing Task C...")
		time.Sleep(150 * time.Millisecond)
		fmt.Println("[EXAMPLE] Task C Completed.")
		return nil
	}})
	if errC != nil {
		fmt.Printf("[EXAMPLE] Error creating task C: %v\n", errC)
		return
	}

	taskD, errD := idempipe.NewTask(idempipe.TaskOptions{Name: "D", Prerequisites: []*idempipe.Task{taskB, taskC}, Function: func(ctx context.Context) error {
		fmt.Println("[EXAMPLE] Executing Task D...")
		time.Sleep(80 * time.Millisecond)
		fmt.Println("[EXAMPLE] Task D Completed.")
		return nil
	}})
	if errD != nil {
		fmt.Printf("[EXAMPLE] Error creating task D: %v\n", errD)
		return
	}

	// Create Pipeline
	pipeline, err := idempipe.NewPipeline(idempipe.Options{
		Name:   "MySimplePipeline",
		Tasks: []*idempipe.Task{taskA, taskB, taskC, taskD},
		// Optional: Provide a custom logger - by default, it will log to standard output via fmt.Print
		// Logger: func(format string) { log.Printf(format) },
		// Optional: Define a default retry strategy for all tasks
		// DefaultRetryStrategy: func(err error, attempt int) error {
		// 	if attempt < 3 {
		// 		fmt.Printf("Default retry strategy: Retrying after error: %v (attempt %d)\n", err, attempt)
		// 		time.Sleep(time.Duration(attempt+1) * 50 * time.Millisecond) // Exponential backoff
		// 		return nil // Indicate retry should proceed
		// 	}
		// 	fmt.Printf("Default retry strategy: Max retries reached for error: %v\n", err)
		// 	return err // Indicate retry should stop
		// },
		// Optional: Set concurrency limit
		// ConcurrencyLimit: 2,
	})
	if err != nil {
		fmt.Printf("[EXAMPLE] Error creating pipeline: %v\n", err)
		return
	}

	// Run Pipeline
	fmt.Println("[EXAMPLE] Starting pipeline...")
	err = pipeline.Run(context.Background())
	if err != nil {
		fmt.Printf("[EXAMPLE] Pipeline finished with error: %v\n", err)
	} else {
		fmt.Println("[EXAMPLE] Pipeline finished successfully.")
	}
}


```

### Example Output 

```
$ go run main.go
[EXAMPLE] Starting pipeline...
2025/04/18 06:03:19.035 UTC [Pipeline MySimplePipeline] Starting pipeline execution...
2025/04/18 06:03:19.035 UTC [Pipeline MySimplePipeline] Task C: Waiting for prerequisite A...
2025/04/18 06:03:19.035 UTC [Pipeline MySimplePipeline] Task D: Waiting for prerequisite B...
2025/04/18 06:03:19.035 UTC [Pipeline MySimplePipeline] Task B: Waiting for prerequisite A...
2025/04/18 06:03:19.035 UTC [Pipeline MySimplePipeline] Task A: Starting...
[EXAMPLE] Executing Task A...
[EXAMPLE] Task A Completed.
2025/04/18 06:03:19.135 UTC [Pipeline MySimplePipeline] Task A: Completed
2025/04/18 06:03:19.135 UTC [Pipeline MySimplePipeline] Task C: Prerequisite A completed successfully.
2025/04/18 06:03:19.135 UTC [Pipeline MySimplePipeline] Task C: Starting...
[EXAMPLE] Executing Task C...
2025/04/18 06:03:19.135 UTC [Pipeline MySimplePipeline] Task B: Prerequisite A completed successfully.
2025/04/18 06:03:19.135 UTC [Pipeline MySimplePipeline] Task B: Starting...
[EXAMPLE] Executing Task B...
[EXAMPLE] Task B is failing once intentionally.
[EXAMPLE] RetryStrategy for Task B: Retrying after error 'simulated temporary failure' (attempt 1).
2025/04/18 06:03:19.186 UTC [Pipeline MySimplePipeline] Task(B): Retrying (attempt 1) after error: simulated temporary failure
2025/04/18 06:03:19.186 UTC [Pipeline MySimplePipeline] Task B: Starting...
[EXAMPLE] Executing Task B...
[EXAMPLE] Task B Completed successfully on retry.
2025/04/18 06:03:19.237 UTC [Pipeline MySimplePipeline] Task B: Completed
2025/04/18 06:03:19.237 UTC [Pipeline MySimplePipeline] Task D: Prerequisite B completed successfully.
2025/04/18 06:03:19.237 UTC [Pipeline MySimplePipeline] Task D: Waiting for prerequisite C...
[EXAMPLE] Task C Completed.
2025/04/18 06:03:19.287 UTC [Pipeline MySimplePipeline] Task C: Completed
2025/04/18 06:03:19.287 UTC [Pipeline MySimplePipeline] Task D: Prerequisite C completed successfully.
2025/04/18 06:03:19.287 UTC [Pipeline MySimplePipeline] Task D: Starting...
[EXAMPLE] Executing Task D...
[EXAMPLE] Task D Completed.
2025/04/18 06:03:19.368 UTC [Pipeline MySimplePipeline] Task D: Completed
2025/04/18 06:03:19.368 UTC [Pipeline MySimplePipeline] All tasks completed successfully.
[EXAMPLE] Pipeline finished successfully.
```

## Contributing

Contributions, suggestions, and feedback are welcome! If you find this library useful or have ideas for improvement, feel free to open an issue or a pull request.

That said, the core goal of this project is to remain simple and lightweight. To keep complexity low:

- Breaking changes will only be considered if absolutely necessary.
- Features that are better served by more complex libraries will likely not be accepted.

If you’re unsure whether your idea fits, feel free to start a discussion before submitting code.

## License

**License**: MPL-2.0

You can use this library in both open-source and commercial projects. If you modify any of the source files, you must share those changes under the same license. You don't need to open source your entire project.

**This is a human-friendly summary, not a substitute for the full license text. See [LICENSE](./LICENSE) for details.**
