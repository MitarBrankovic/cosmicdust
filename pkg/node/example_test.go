package node_test

import (
	"context"
	"log"
	"os"
	"os/signal"
	"runtime/debug"
	"syscall"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/c12s/cosmicdust/pkg/node"
)

// An agent that already runs on every node embeds cosmicdust: it follows Docker and
// containerd, keeps the logs under /var/lib/cosmicdust, and serves the cosmicdust_agent_*
// metrics with its own.
func Example() {
	debug.SetMemoryLimit(64 << 20) // the process's limit, see "Memory"
	reg := prometheus.NewRegistry()
	n, err := node.New(node.Options{
		DataDir:    "/var/lib/cosmicdust",
		Docker:     &node.DockerOptions{},
		Containerd: &node.ContainerdOptions{},
		CgroupRoot: "/sys/fs/cgroup",
		Registerer: reg,
	})
	if err != nil {
		log.Fatal(err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := n.Run(ctx); err != nil {
		log.Fatal(err)
	}
}
