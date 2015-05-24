package taskcore

import (
	"log"
	"net"

	"github.com/coreos/go-etcd/etcd"
	"github.com/taskgraph/taskcore/pkg/grpcutil"
	pb "github.com/taskgraph/taskcore/proto"
	"golang.org/x/net/context"
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
	cc, err := grpcutil.GetConn(f.EtcdClient, f.JobName, id)
	if err != nil {
		// TODO: retry
		f.Logger.Panic(err)
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
