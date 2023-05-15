package jobsite

import (
	"context"
	"errors"
	"log"
	"os"
	"runtime"
	"runtime/debug"
	"sync"
	"sync/atomic"
)

var ErrStopped = errors.New(`jobsite: stopped`)

type Func func()

// JobSite is a job pool's.
type JobSite interface {
	Closed() bool
	Call(ctx context.Context, task Func) error
	Stop()
}

type engine struct {
	running int32
	closed  chan struct{}
	wait    *sync.WaitGroup
	cond    *sync.Cond
	log     *log.Logger
	chs     chan chan Func
}

func (e *engine) Closed() bool {
	return atomic.LoadInt32(&e.running) != 1
}

func (e *engine) Call(ctx context.Context, task Func) error {
	if e.Closed() {
		return ErrStopped
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case ch := <-e.chs:
		ch <- task
	}
	return nil
}

func (e *engine) Stop() {
	if atomic.SwapInt32(&e.running, 0) != 1 {
		return
	}
	close(e.closed)
	e.cond.Broadcast()
	e.wait.Wait()
}

func (e *engine) run() {
	e.wait.Add(1)
	defer e.wait.Done()
	ch := make(chan Func)
	defer close(ch)
EXIT:
	for !e.Closed() {
		e.cond.L.Lock()
		e.chs <- ch
		e.cond.Wait()
		e.cond.L.Unlock()
		select {
		case <-e.closed:
			break EXIT
		case task := <-ch:
			e.recover(task)
		}
	}
}

func (e *engine) recover(task Func) {
	defer func() {
		if err := recover(); err != nil {
			e.log.Printf("%v\r\n%s", err, debug.Stack())
		}
	}()
	task()
}

// New return a job pool's
func New(num int, logs ...*log.Logger) JobSite {
	if num <= 0 {
		num = runtime.NumCPU()
	}
	if len(logs) == 0 {
		logs = append(logs, log.New(os.Stdout, ``, log.LstdFlags|log.Lmicroseconds|log.Lmsgprefix))
	}
	site := &engine{
		running: 1,
		closed:  make(chan struct{}),
		wait:    new(sync.WaitGroup),
		cond:    sync.NewCond(new(sync.Mutex)),
		log:     logs[0],
		chs:     make(chan chan Func, num),
	}
	site.log.SetPrefix(`[jobsite] `)
	for i := 0; i < num; i++ {
		go site.run()
	}
	runtime.SetFinalizer(site, func(e *engine) {
		e.Stop()
		close(e.chs)
	})
	return site
}
