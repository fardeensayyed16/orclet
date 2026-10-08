package worker

import (
	"context"
	"fmt"
	"log"
	"sync"

	"github.com/moby/moby/client"
)

type DockerClientManager struct {
	mu           sync.RWMutex
	cli          *client.Client
	dockerSocket string
}

func NewDockerClientManager(dockerSocket string) *DockerClientManager {
	return &DockerClientManager{
		dockerSocket: dockerSocket,
	}
}

func (m *DockerClientManager) InitClient() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	opts := []client.Opt{client.FromEnv}
	if m.dockerSocket != "" && m.dockerSocket != "auto" {
		opts = append(opts, client.WithHost(m.dockerSocket))
	}

	cli, err := client.NewClientWithOpts(opts...)
	if err != nil {
		return err
	}

	m.cli = cli
	return nil
}

func (m *DockerClientManager) CheckHealth(ctx context.Context) (int64, int64, bool, error) {
	m.mu.RLock()
	cli := m.cli
	m.mu.RUnlock()

	if cli == nil {
		if err := m.InitClient(); err != nil {
			return 0, 0, false, nil
		}
		m.mu.RLock()
		cli = m.cli
		m.mu.RUnlock()
	}

	res, err := cli.Info(ctx, client.InfoOptions{})
	if err != nil {
		log.Printf("Docker ping/info error: %v (will retry docker connection)", err)
		_ = m.InitClient()
		return 0, 0, false, nil
	}

	info := res.Info
	if info.OSType != "linux" {
		return 0, 0, false, fmt.Errorf("refusing registration: docker OSType is %q, must be \"linux\"", info.OSType)
	}

	cpuMillis := int64(info.NCPU * 1000)
	memBytes := info.MemTotal

	return cpuMillis, memBytes, true, nil
}

