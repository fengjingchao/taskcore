package taskcore

import "golang.org/x/net/context"

type TaskCore interface {
	Init(ctx context.Context, id uint64)
	Process(ctx context.Context, id uint64, data []byte)
}
