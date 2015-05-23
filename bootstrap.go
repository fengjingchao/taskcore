package taskcore

import (
	"github.com/taskgraph/taskgraph/pkg/etcdutil"
	"golang.org/x/net/context"
)

type Bootstrap interface {
	Start(ctx context.Context) error
}

func (f *Framework) AcquireTask(ctx context.Context) error {
	if err := f.occupyTask(); err != nil {
		f.Logger.Panicf("occupyTask() failed: %v", err)
	}
	f.TaskCore.Init(context.Background(), f.TaskID)
	return nil
}
func (f *Framework) Start(ctx context.Context) error {
	err := f.AcquireTask(ctx)
	if err != nil {
		return err
	}
	return f.Run()
}

func (f *Framework) Run() error {
	// run server.
	f.startRPC()
	// NOTE: graceful shutdown.
	// only returns user setup error.
	return nil
}

// acquire task, register itself.
func (f *Framework) occupyTask() error {
	for {
		freeTask, err := etcdutil.WaitFreeTask(f.EtcdClient, f.JobName, f.Logger)
		if err != nil {
			return err
		}
		f.Logger.Printf("standby grabbed free task %d", freeTask)
		ok, err := etcdutil.TryOccupyTask(f.EtcdClient, f.JobName, freeTask, f.ln.Addr().String())
		if err != nil {
			return err
		}
		if ok {
			f.TaskID = freeTask
			return nil
		}
		f.Logger.Printf("standby tried task %d failed. Wait free task again.", freeTask)
	}
}
