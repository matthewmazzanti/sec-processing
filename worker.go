package main

import (
	"sync"
)

type WorkFunc[In any, Out any] func(In) (Out, error)

type Tagged[Tag any, Value any] struct {
	Tag Tag
	Value Value
}

func tag[Tag any, Value any](tag Tag, value Value) Tagged[Tag, Value] {
	return Tagged[Tag, Value]{
		Tag: tag,
		Value: value,
	}
}

type WorkerPool[In any, Out any, Tag any] struct {
	in <-chan Tagged[Tag, In]
	out chan<- Tagged[Tag, Out]
	err chan<- Tagged[Tag, error]
	wg *sync.WaitGroup
}

// Create a new runner. This returns the runner to interact with, and the input
// and output channels to send data to
func NewWorkerPool[In any, Out any, Tag any](
	in <-chan Tagged[Tag, In],
	fn WorkFunc[In, Out],
	count int,
	buffer int,
) (
	<-chan Tagged[Tag, Out],
	<-chan Tagged[Tag, error],
) {
	out := make(chan Tagged[Tag, Out], buffer)
	err := make(chan Tagged[Tag, error], buffer)
	runner := WorkerPool[In, Out, Tag]{
		in: in,
		out: out,
		err: err,
		wg: &sync.WaitGroup{},
	}
	runner.runCount(count, fn)

	return out, err
}

// Start up n jobs to run the work
func (r *WorkerPool[In, Out, Tag]) runCount(n int, fn WorkFunc[In, Out]) {
	r.wg.Add(n)
	for i := 0; i < n; i++ {
		go r.run(fn)
	}

	go func() {
		r.wg.Wait()
		close(r.out)
		close(r.err)
	}()
}

func (r *WorkerPool[In, Out, Tag]) run(fn WorkFunc[In, Out]) {
	// Recieve filename from input channel
	for in := range r.in {
		out, err := fn(in.Value)
		if err != nil {
			r.err <- tag(in.Tag, err)
			continue
		}

		// Send result over output channel
		r.out <- tag(in.Tag, out)
	}

	// Notify that the work has completed. This executes once the filenames
	// channel is closed and the loop exits
	r.wg.Done()
}
