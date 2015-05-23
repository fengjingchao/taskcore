package master

import (
	"github.com/taskgraph/taskcore"
	"golang.org/x/net/context"
)

type Master interface {
	Record(ctx context.Context, id uint64, state []byte)
	Retrieve(ctx context.Context, id uint64) (state []byte)
}

type TaskMaster interface {
	taskcore.TaskCore
	Master
}

func MasterBootstrap() taskcore.Bootstrap {
	panic("")
}
