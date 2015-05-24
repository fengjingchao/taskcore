package taskcore

import (
	pb "github.com/taskgraph/taskcore/proto"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

func (f *Framework) startRPC() {
	f.Logger.Printf("serving grpc on %s\n", f.ln.Addr())
	s := grpc.NewServer()
	pb.RegisterCommunicationServer(s, f)
	s.Serve(f.ln)
}

func (f *Framework) Process(ctx context.Context, req *pb.Message) (*pb.Message, error) {
	go f.TaskCore.Process(ctx, req.Id, req.Data)
	return new(pb.Message), nil
}
