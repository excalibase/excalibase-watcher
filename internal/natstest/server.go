//go:build integration

// Package natstest runs a JetStream server in a container that a test can take
// down and bring back on the same address, the way a NATS restart or an outage
// does. Streams on file storage survive the restart.
package natstest

import (
	"context"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/go-connections/nat"
	"github.com/nats-io/nats.go"
	"github.com/testcontainers/testcontainers-go"
	tcnats "github.com/testcontainers/testcontainers-go/modules/nats"
)

const image = "nats:2.10"

type Server struct {
	URL string

	container *tcnats.NATSContainer
	running   bool
}

// Start returns a running server.
func Start(t *testing.T) *Server {
	t.Helper()
	s := Reserve(t)
	s.Start(t)
	return s
}

// Reserve creates the server on a fixed host port without starting it, so a
// client can be pointed at a server that is down.
func Reserve(t *testing.T) *Server {
	t.Helper()
	port := freePort(t)
	natsContainer, err := tcnats.Run(context.Background(), image,
		testcontainers.WithHostConfigModifier(func(hostConfig *container.HostConfig) {
			hostConfig.PortBindings = nat.PortMap{
				"4222/tcp": {{HostIP: "127.0.0.1", HostPort: strconv.Itoa(port)}},
			}
		}))
	if err != nil {
		t.Fatalf("start nats container: %v", err)
	}
	t.Cleanup(func() {
		if err := testcontainers.TerminateContainer(natsContainer); err != nil {
			t.Logf("terminate nats container: %v", err)
		}
	})
	s := &Server{
		URL:       "nats://" + net.JoinHostPort("127.0.0.1", strconv.Itoa(port)),
		container: natsContainer,
		running:   true,
	}
	s.Stop()
	return s
}

// Start brings the server up on its reserved address.
func (s *Server) Start(t *testing.T) {
	t.Helper()
	if s.running {
		return
	}
	if err := s.container.Start(context.Background()); err != nil {
		t.Fatalf("start nats: %v", err)
	}
	s.running = true
	waitReachable(t, s.URL)
}

// Stop takes the server down; every client loses its connection.
func (s *Server) Stop() {
	if !s.running {
		return
	}
	timeout := 5 * time.Second
	_ = s.container.Stop(context.Background(), &timeout)
	s.running = false
}

// Connect opens a reader connection that rides out Stop and Start.
func (s *Server) Connect(t *testing.T) *nats.Conn {
	t.Helper()
	conn, err := nats.Connect(s.URL,
		nats.MaxReconnects(-1), nats.ReconnectWait(200*time.Millisecond), nats.Timeout(5*time.Second))
	if err != nil {
		t.Fatalf("reader connect: %v", err)
	}
	t.Cleanup(conn.Close)
	return conn
}

func freePort(t *testing.T) int {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve port: %v", err)
	}
	defer listener.Close()
	return listener.Addr().(*net.TCPAddr).Port
}

func waitReachable(t *testing.T, url string) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if conn, err := nats.Connect(url, nats.Timeout(time.Second)); err == nil {
			conn.Close()
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("nats at %s did not come back", url)
}
