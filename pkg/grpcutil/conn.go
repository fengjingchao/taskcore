package grpcutil

import (
	"fmt"

	"github.com/coreos/go-etcd/etcd"
	"github.com/taskgraph/taskgraph/pkg/etcdutil"
	"google.golang.org/grpc"
)

func GetConn(etcdClient *etcd.Client, jobName string, id uint64) (*grpc.ClientConn, error) {
	addr, err := etcdutil.GetAddress(etcdClient, jobName, id)
	if err != nil {
		return nil, fmt.Errorf("getAddress(%d) failed: %v", id, err)
	}
	cc, err := grpc.Dial(addr)
	if err != nil {
		return nil, fmt.Errorf("grpc.Dial to task %d (addr: %s) failed: %v", id, addr, err)
	}
	return cc, nil
}
