package master

import (
	"log"
	"net"

	"github.com/taskgraph/taskcore"

	pb "github.com/taskgraph/taskcore/master/proto"
	"github.com/taskgraph/taskcore/pkg/grpcutil"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

const (
	masterID = 0
)

type FrameworkWithMaster struct {
	taskcore.Framework
	Master
	TaskMaster
	ln net.Listener // master rpc server listener
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

func (f *FrameworkWithMaster) Record(ctx context.Context, id uint64, state []byte) error {
	cc, err := grpcutil.GetConn(f.EtcdClient, f.JobName, masterID)
	if err != nil {
		// TODO: retry
		f.Logger.Panic(err)
	}
	defer cc.Close()
	c := pb.NewStateServerClient(cc)
	req := &pb.RecordRequest{
		Id:    id,
		State: state,
	}
	_, err = c.RecordRPC(ctx, req)
	if err != nil {
		f.Logger.Panicf("should retry on networking error")
	}
	return nil
}

func (f *FrameworkWithMaster) Retrieve(ctx context.Context, id uint64) ([]byte, error) {
	cc, err := grpcutil.GetConn(f.EtcdClient, f.JobName, masterID)
	if err != nil {
		// TODO: retry
		f.Logger.Panic(err)
	}
	defer cc.Close()
	c := pb.NewStateServerClient(cc)
	req := &pb.RetrieveRequest{
		Id: id,
	}
	reply, err := c.RetrieveRPC(ctx, req)
	if err != nil {
		f.Logger.Panicf("should retry on networking error")
	}
	return reply.State, nil
}

func (f *FrameworkWithMaster) RecordRPC(ctx context.Context, req *pb.RecordRequest) (reply *pb.RecordReply, err error) {
	f.TaskMaster.Record(ctx, req.Id, req.State)
	return new(pb.RecordReply), nil
}

func (f *FrameworkWithMaster) RetrieveRPC(ctx context.Context, req *pb.RetrieveRequest) (reply *pb.RetrieveReply, err error) {
	reply = new(pb.RetrieveReply)
	reply.State = f.TaskMaster.Retrieve(ctx, req.Id)
	return reply, nil
}
