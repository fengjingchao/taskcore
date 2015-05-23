package master

import (
	"log"
	"net"

	"github.com/taskgraph/taskcore"

	pb "github.com/taskgraph/taskcore/master/proto"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

type FrameworkWithMaster struct {
	taskcore.Framework
	Master
	states map[uint64][]byte
	ln     net.Listener // master rpc server listener
}

type framework struct {
	taskcore.Framework
	logger *log.Logger
}

func (f *FrameworkWithMaster) Start(ctx context.Context) error {
	err := f.AcquireTask(ctx)
	if err != nil {
		return err
	}
	if f.Framework.TaskID == 0 {
		go f.startMasterRPC()
	}
	return f.Run()
}

func (f *FrameworkWithMaster) startMasterRPC() {
	f.Logger.Printf("serving master grpc on %s\n", f.ln.Addr())
	s := grpc.NewServer()
	pb.RegisterStateServerServer(s, f)
	s.Serve(f.ln)
}

func (f *FrameworkWithMaster) Record(ctx context.Context, id uint64, state []byte) {
	// find master
	// call rpc
}

func (f *FrameworkWithMaster) Retrieve(ctx context.Context, id uint64) (state []byte) {
	// find master
	// call rpc
	return nil
}

func (f *FrameworkWithMaster) RecordRPC(context.Context, *pb.RecordRequest) (*pb.RecordReply, error) {
	return new(pb.RecordReply), nil
}

func (f *FrameworkWithMaster) RetrieveRPC(context.Context, *pb.RetrieveRequest) (reply *pb.RetrieveReply, err error) {

	return reply, nil
}
