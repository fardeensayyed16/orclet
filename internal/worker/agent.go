package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/fardeensayyed16/orclet/internal/runtime"
	"github.com/fardeensayyed16/orclet/pkg/config"
)

type WorkerAgent struct {
	cfg            *config.Config
	dockerMgr      *DockerClientManager
	docker         runtime.DockerRuntime
	portPool       *portPool
	httpClient     *http.Client
	observationsMu sync.RWMutex
	observations   map[string]replicaObservation
	restartMu      sync.Mutex
	restarts       map[string]restartState
	reconcileMu    sync.Mutex
	desiredMu      sync.RWMutex
	desired        map[string]desiredReplica
	probeMu        sync.Mutex
	probes         map[string]probeState
}

func NewWorkerAgent(cfg *config.Config) *WorkerAgent {
	return &WorkerAgent{
		cfg:          cfg,
		dockerMgr:    NewDockerClientManager(cfg.DockerSocket),
		observations: make(map[string]replicaObservation),
		restarts:     make(map[string]restartState),
		desired:      make(map[string]desiredReplica),
		probes:       make(map[string]probeState),
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

func (w *WorkerAgent) Run(ctx context.Context) error {
	// Node name resolution
	nodeName := w.cfg.Name
	if nodeName == "" {
		hostname, err := os.Hostname()
		if err != nil {
			nodeName = "orclet-node"
		} else {
			nodeName = hostname
		}
	}
	w.cfg.Name = nodeName

	address, err := resolveNodeAddress(w.cfg.Address, w.cfg.Server)
	if err != nil {
		return err
	}
	w.cfg.Address = address
	w.docker, err = runtime.NewDockerRuntime(w.cfg.DockerSocket)
	if err != nil {
		return err
	}
	if w.cfg.HostPortRange == "" {
		w.cfg.HostPortRange = "40000-41000"
	}
	w.portPool, err = newPortPool(w.cfg.HostPortRange)
	if err != nil {
		return err
	}
	defer w.portPool.close()

	// 1. One-shot Registration with retry/backoff
	if err := w.registerWithBackoff(ctx); err != nil {
		return err
	}

	go w.workLoop(ctx)
	go w.healthLoop(ctx)
	go w.statsLoop(ctx)
	w.heartbeatLoop(ctx)
	return nil
}

func (w *WorkerAgent) registerWithBackoff(ctx context.Context) error {
	backoff := []time.Duration{1 * time.Second, 5 * time.Second, 10 * time.Second, 30 * time.Second}
	backoffIdx := 0

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Re-check Docker health & capacities before each registration attempt
		cpuMillis, memBytes, dockerOK, err := w.dockerMgr.CheckHealth(ctx)
		if err != nil {
			log.Printf("Docker health check error: %v", err)
		}

		if !dockerOK {
			log.Printf("Docker socket not reachable yet; retrying health check before register...")
		} else {
			// Apply config overrides if set
			if w.cfg.CapacityCPU > 0 {
				cpuMillis = w.cfg.CapacityCPU
			}
			if w.cfg.CapacityMemory > 0 {
				memBytes = w.cfg.CapacityMemory
			}

			err = w.registerOnce(ctx, cpuMillis, memBytes)
			if err == nil {
				log.Printf("Successfully registered node %s (CPU: %dm, Mem: %d bytes) with master %s", w.cfg.Name, cpuMillis, memBytes, w.cfg.Server)
				return nil
			}

			if strings.Contains(err.Error(), "HTTP 409") {
				log.Printf("Registration conflict (409): %v. Exiting worker agent process.", err)
				os.Exit(1)
			}

			if strings.Contains(err.Error(), "HTTP 401") {
				log.Printf("Unauthorized (401) during registration — check --token flag or ORCLET_TOKEN")
			} else {
				log.Printf("Registration failed: %v", err)
			}
		}

		sleepTime := backoff[backoffIdx]
		if backoffIdx < len(backoff)-1 {
			backoffIdx++
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(sleepTime):
		}
	}
}

type registerReq struct {
	Name                string `json:"name"`
	Address             string `json:"address"`
	CapacityCPUMillis   int64  `json:"capacity_cpu_millis"`
	CapacityMemoryBytes int64  `json:"capacity_memory_bytes"`
	Runtime             string `json:"runtime"`
}

func (w *WorkerAgent) registerOnce(ctx context.Context, cpuMillis, memBytes int64) error {
	serverURL := w.cfg.Server
	if !strings.HasPrefix(serverURL, "http://") && !strings.HasPrefix(serverURL, "https://") {
		serverURL = "http://" + serverURL
	}
	url := fmt.Sprintf("%s/api/v1/nodes/register", serverURL)

	reqBody := registerReq{
		Name:                w.cfg.Name,
		Address:             w.cfg.Address,
		CapacityCPUMillis:   cpuMillis,
		CapacityMemoryBytes: memBytes,
		Runtime:             "docker",
	}

	bodyBytes, err := json.Marshal(reqBody)
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(bodyBytes))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if w.cfg.Token != "" {
		req.Header.Set("Authorization", "Bearer "+w.cfg.Token)
	}

	resp, err := w.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusCreated {
		return nil
	}

	return fmt.Errorf("server returned HTTP %d: %s", resp.StatusCode, string(respBody))
}

func (w *WorkerAgent) heartbeatLoop(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := w.sendHeartbeat(ctx); err != nil {
				log.Printf("Heartbeat error: %v", err)
				if strings.Contains(err.Error(), "HTTP 404") {
					log.Printf("Node missing from daemon (404); re-registering...")
					_ = w.registerWithBackoff(ctx)
				} else if strings.Contains(err.Error(), "HTTP 409") {
					log.Printf("Address conflict (409) during heartbeat. Exiting worker process.")
					os.Exit(1)
				}
			}
		}
	}
}

type heartbeatReq struct {
	Address             string               `json:"address"`
	ContainerIDs        []string             `json:"container_ids"`
	Replicas            []replicaObservation `json:"replicas"`
	DockerOK            bool                 `json:"docker_ok"`
	CapacityCPUMillis   int64                `json:"capacity_cpu_millis,omitempty"`
	CapacityMemoryBytes int64                `json:"capacity_memory_bytes,omitempty"`
}

func (w *WorkerAgent) sendHeartbeat(ctx context.Context) error {
	cpuMillis, memBytes, dockerOK, err := w.dockerMgr.CheckHealth(ctx)
	if err != nil {
		dockerOK = false
	}
	if w.cfg.CapacityCPU > 0 {
		cpuMillis = w.cfg.CapacityCPU
	}
	if w.cfg.CapacityMemory > 0 {
		memBytes = w.cfg.CapacityMemory
	}

	serverURL := w.cfg.Server
	if !strings.HasPrefix(serverURL, "http://") && !strings.HasPrefix(serverURL, "https://") {
		serverURL = "http://" + serverURL
	}
	url := fmt.Sprintf("%s/api/v1/nodes/%s/heartbeat", serverURL, w.cfg.Name)
	replicas := w.collectReplicaHeartbeats(ctx)
	containerIDs := make([]string, 0, len(replicas))
	for _, replica := range replicas {
		if replica.ContainerID != "" {
			containerIDs = append(containerIDs, replica.ContainerID)
		}
	}

	reqBody := heartbeatReq{
		Address:             w.cfg.Address,
		ContainerIDs:        containerIDs,
		Replicas:            replicas,
		DockerOK:            dockerOK,
		CapacityCPUMillis:   cpuMillis,
		CapacityMemoryBytes: memBytes,
	}

	bodyBytes, err := json.Marshal(reqBody)
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(bodyBytes))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if w.cfg.Token != "" {
		req.Header.Set("Authorization", "Bearer "+w.cfg.Token)
	}

	resp, err := w.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode == http.StatusOK {
		return nil
	}

	return fmt.Errorf("server returned HTTP %d: %s", resp.StatusCode, string(respBody))
}
