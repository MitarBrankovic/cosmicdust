//go:build integration

// Lifecycle scenarios against the node's real dockerd and containerd. Docker
// needs the docker group; containerd needs root, and without it only Docker is
// followed:
//
//	go test -tags integration ./internal/lifecycle/
//	sudo -E env PATH=$PATH go test -tags integration ./internal/lifecycle/
package lifecycle

import (
	"context"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	ctrdsource "github.com/c12s/cosmicdust/internal/runtime/containerd"
	dockersource "github.com/c12s/cosmicdust/internal/runtime/docker"
	"github.com/c12s/cosmicdust/pkg/cosmicdust"
)

// dockerCLI returns what the command printed on stdout. Progress (an image
// pull on a fresh machine) goes to stderr and is shown only on failure.
func dockerCLI(t *testing.T, args ...string) string {
	t.Helper()
	var stderr strings.Builder
	cmd := exec.Command("docker", args...)
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("docker %s: %v\n%s%s", strings.Join(args, " "), err, out, stderr.String())
	}
	return strings.TrimSpace(string(out))
}

// runContainer runs one and removes it when the test ends.
func runContainer(t *testing.T, name string, args ...string) string {
	t.Helper()
	_ = exec.Command("docker", "rm", "-f", name).Run()
	out := dockerCLI(t, append([]string{"run", "-d", "--name", name}, args...)...)
	t.Cleanup(func() { _ = exec.Command("docker", "rm", "-f", name).Run() })
	lines := strings.Split(out, "\n")
	return lines[len(lines)-1] // the container ID
}

func eventually(t *testing.T, tr *Tracker, id string, within time.Duration, ok func(Container) bool) Container {
	t.Helper()
	deadline := time.Now().Add(within)
	for {
		c, _ := tr.Get(id)
		if ok(c) {
			return c
		}
		if time.Now().After(deadline) {
			t.Fatalf("container %s: still %s epoch %d exit %v oom %v after %s", short(id), c.State, c.Epoch,
				deref(c.ExitCode), c.OOMKilled, within)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func TestIntegration(t *testing.T) {
	if err := exec.Command("docker", "info").Run(); err != nil {
		t.Skip("no docker daemon:", err)
	}
	d, err := dockersource.New(dockersource.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = d.Close() }()
	sources := []Source{{EventSource: d}}
	runtimes := []string{cosmicdust.RuntimeDocker}
	if os.Geteuid() == 0 {
		c, err := ctrdsource.New(ctrdsource.Options{})
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = c.Close() }()
		sources = append(sources, Source{EventSource: c})
		runtimes = append(runtimes, cosmicdust.RuntimeContainerd)
	} else {
		t.Log("not root: containerd is not followed")
	}

	h := start(t, Options{Sources: sources, CgroupRoot: "/sys/fs/cgroup"})
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() { // keep Out drained
		for {
			select {
			case <-h.out:
			case <-ctx.Done():
				return
			}
		}
	}()
	h.reconciled(runtimes...)

	t.Run("stop", func(t *testing.T) {
		id := runContainer(t, "it-stop", "alpine", "sleep", "60")
		eventually(t, h.tr, id, 5*time.Second, func(c Container) bool { return c.State == cosmicdust.StateRunning && c.Epoch == 1 })
		dockerCLI(t, "stop", "-t", "1", id)
		c := eventually(t, h.tr, id, 5*time.Second, func(c Container) bool { return c.State == cosmicdust.StateExited })
		if c.Epoch != 1 || deref(c.ExitCode) != 137 || c.OOMKilled || c.Name != "it-stop" || c.LogPath == "" {
			t.Fatalf("after stop: %+v", c)
		}
		dockerCLI(t, "rm", id)
		eventually(t, h.tr, id, 5*time.Second, func(c Container) bool { return c.State == cosmicdust.StateRemoved })
	})

	t.Run("restart policy", func(t *testing.T) {
		id := runContainer(t, "it-restart", "--restart=on-failure:3", "alpine", "sh", "-c", "exit 1")
		c := eventually(t, h.tr, id, 30*time.Second, func(c Container) bool { return c.Epoch == 4 && c.State == cosmicdust.StateExited })
		if deref(c.ExitCode) != 1 {
			t.Fatalf("exit code %v, want 1", deref(c.ExitCode))
		}
		if got := dockerCLI(t, "inspect", "-f", "{{.RestartCount}}", id); got != "3" {
			t.Fatalf("docker counts %s restarts, the tracker 4 incarnations", got)
		}
	})

	// The kernel's word on the kill can come after the exit, so the test
	// waits for both. A 6 MB container is killed within milliseconds of its
	// start: the runtimes have been seen to miss that, and the container's
	// own counter is often gone before it can be read again; its kill is
	// read from the parent cgroup. That needs the task's create, so root.
	for _, tc := range []struct{ name, limit, mb string }{
		{"oom", "64m", "500"},
		{"oom within milliseconds", "6m", "50"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.limit == "6m" && os.Geteuid() != 0 {
				t.Skip("needs containerd's /tasks/create, which needs root")
			}
			id := runContainer(t, "it-oom-"+tc.limit, "-m", tc.limit, "--memory-swap", tc.limit, "alpine",
				"sh", "-c", "dd if=/dev/zero of=/dev/shm/fill bs=1M count="+tc.mb)
			c := eventually(t, h.tr, id, 10*time.Second, func(c Container) bool {
				return c.State == cosmicdust.StateExited && c.OOMKilled
			})
			if deref(c.ExitCode) != 137 {
				t.Fatalf("after OOM: %+v", c)
			}
		})
	}

	t.Run("kill -9 is not an OOM", func(t *testing.T) {
		id := runContainer(t, "it-kill", "alpine", "sleep", "60")
		eventually(t, h.tr, id, 5*time.Second, func(c Container) bool { return c.State == cosmicdust.StateRunning })
		dockerCLI(t, "kill", "-s", "KILL", id)
		c := eventually(t, h.tr, id, 5*time.Second, func(c Container) bool { return c.State == cosmicdust.StateExited })
		if c.OOMKilled || deref(c.ExitCode) != 137 {
			t.Fatalf("after kill -9: %+v", c)
		}
	})

	t.Run("health checks do not end the container", func(t *testing.T) {
		id := runContainer(t, "it-health", "--health-cmd", "true", "--health-interval", "1s", "alpine", "sleep", "60")
		eventually(t, h.tr, id, 5*time.Second, func(c Container) bool { return c.State == cosmicdust.StateRunning })
		time.Sleep(4 * time.Second) // four health checks, four exec exits
		if c, _ := h.tr.Get(id); c.State != cosmicdust.StateRunning || c.Epoch != 1 {
			t.Fatalf("after health checks: %s epoch %d", c.State, c.Epoch)
		}
	})

	t.Run("application from compose labels", func(t *testing.T) {
		id := runContainer(t, "it-compose", "-l", "com.docker.compose.project=shop", "-l", "com.docker.compose.service=api",
			"alpine", "sleep", "60")
		c := eventually(t, h.tr, id, 5*time.Second, func(c Container) bool { return c.State == cosmicdust.StateRunning })
		if c.App != "shop.api" {
			t.Fatalf("app %q, want shop.api", c.App)
		}
	})
}
