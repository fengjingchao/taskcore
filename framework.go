package taskcore

import (
	"log"
	"net"

	"github.com/coreos/go-etcd/etcd"
	pb "github.com/taskgraph/taskcore/proto"
	"github.com/taskgraph/taskgraph/pkg/etcdutil"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

type Framework struct {
	JobName string
	TaskCore
	*log.Logger
	TaskID     uint64
	EtcdClient *etcd.Client
	ln         net.Listener
}

func (f *Framework) Send(ctx context.Context, id uint64, data []byte) error {
	addr, err := etcdutil.GetAddress(f.EtcdClient, f.JobName, id)
	if err != nil {
		// TODO: retry
		f.Logger.Panicf("getAddress(%d) failed: %v", id, err)
	}
	cc, err := grpc.Dial(addr)
	if err != nil {
		f.Logger.Panicf("grpc.Dial to task %d (addr: %s) failed: %v", id, addr, err)
	}
	defer cc.Close()
	c := pb.NewCommunicationClient(cc)
	input := &pb.Message{
		Id:   f.TaskID,
		Data: data,
	}
	_, err = c.Process(ctx, input)
	if err != nil {
		f.Logger.Panicf("should retry on networking error")
	}
	return nil
}
