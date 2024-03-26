package storage

import (
	"context"
	"fmt"
	"sync/atomic"

	"golang.org/x/sync/errgroup"
)

// Result represents the Size function result
type Result struct {
	// Total Size of File objects
	Size int64
	// Count is a count of File objects processed
	Count int64
}

type DirSizer interface {
	// Size calculate a size of given Dir, receive a ctx and the root Dir instance
	// will return Result or error if happened
	Size(ctx context.Context, d Dir) (Result, error)
}

// sizer implement the DirSizer interface
type sizer struct {
	// maxWorkersCount number of workers for asynchronous run
	maxWorkersCount int
	workerPool      *errgroup.Group
}

const (
	unlimitedMaxWorkers    = -1
	defaultMaxWorkersCount = unlimitedMaxWorkers
)

type DirSizerOptions = func(*sizer)

// NewSizer returns new DirSizer instance
func NewSizer(opts ...DirSizerOptions) DirSizer {
	ds := sizer{
		maxWorkersCount: defaultMaxWorkersCount,
		workerPool:      &errgroup.Group{},
	}
	ds.workerPool.SetLimit(ds.maxWorkersCount)
	for _, opt := range opts {
		opt(&ds)
	}
	return &ds
}

func WithMaxWorkers(workersCount int) DirSizerOptions {
	return func(ds *sizer) {
		ds.maxWorkersCount = workersCount
		ds.workerPool.SetLimit(ds.maxWorkersCount)
	}
}

type ErrGroupTask = func() error

func (a *sizer) statAggregate(ctx context.Context, aggregateF func(int64), f File) ErrGroupTask {
	return func() error {
		if err := ctx.Err(); err != nil {
			return err
		}
		sz, err := f.Stat(ctx)
		if err != nil {
			// return fmt.Errorf("error processing file %v: %w", f.Name(), err) //FIXME: This doesn't work since the generated mock does not implement Name method.
			return fmt.Errorf("error processing file: %w", err)
		}
		aggregateF(sz)
		return nil
	}
}

func (a *sizer) walk(ctx context.Context, aggregateF func(int64), parentD Dir) ErrGroupTask {
	return func() error {
		if err := ctx.Err(); err != nil {
			return err
		}
		dirs, files, err := parentD.Ls(ctx)
		if err != nil {
			// return fmt.Errorf("error listing directory %v: %w", parentD.Name(), err) //FIXME: This doesn't work since the generated mock does not implement Name method.
			return fmt.Errorf("error listing directory: %w", err)
		}

		for _, d := range dirs {
			a.workerPool.Go(a.walk(ctx, aggregateF, d))
		}
		for _, f := range files {
			a.workerPool.Go(a.statAggregate(ctx, aggregateF, f))
		}
		return nil
	}
}

func (a *sizer) Size(ctx context.Context, d Dir) (Result, error) {
	sumSz := atomic.Int64{}
	cnt := atomic.Int64{}

	aggregateF := func(sz int64) {
		sumSz.Add(sz)
		cnt.Add(1)
	}

	a.workerPool.Go(a.walk(ctx, aggregateF, d))

	if err := a.workerPool.Wait(); err != nil {
		// return Result{}, fmt.Errorf("error calculating size of dir %v: %w", d.Name(), err) //FIXME: This doesn't work since the generated mock does not implement Name method.
		return Result{}, fmt.Errorf("error calculating size of dir: %w", err)
	}

	if err := ctx.Err(); err != nil {
		return Result{}, err
	}

	return Result{
		Size:  sumSz.Load(),
		Count: cnt.Load(),
	}, nil
}
