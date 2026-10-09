package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/fardeensayyed16/orclet/internal/runtime"
	"github.com/moby/moby/client"
)

func (w *WorkerAgent) statsLoop(ctx context.Context) {
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			containers, err := w.docker.ContainerList(ctx, client.ContainerListOptions{
				Filters: make(client.Filters).Add("label", replicaLabel),
			})
			if err != nil {
				log.Printf("Stats container list error: %v", err)
				continue
			}

			type target struct {
				replicaID   string
				containerID string
			}
			targets := make([]target, 0, len(containers.Items))
			for _, container := range containers.Items {
				if replicaID := container.Labels[replicaLabel]; replicaID != "" {
					targets = append(targets, target{replicaID: replicaID, containerID: container.ID})
				}
			}

			jobs := make(chan target)
			var workers sync.WaitGroup
			for range min(8, len(targets)) {
				workers.Add(1)
				go func() {
					defer workers.Done()
					for item := range jobs {
						stats, err := runtime.ReadStats(ctx, w.docker, item.containerID)
						if err != nil {
							log.Printf("Stats sampling error for replica %s: %v", item.replicaID, err)
							continue
						}
						if stats.PreRead.IsZero() || stats.Read.IsZero() {
							continue
						}

						cpuUsage := stats.CPUStats.CPUUsage.TotalUsage
						preCPUUsage := stats.PreCPUStats.CPUUsage.TotalUsage
						if cpuUsage < preCPUUsage {
							continue
						}
						deltaCPU := float64(cpuUsage - preCPUUsage)
						deltaTime := float64(stats.Read.UnixNano() - stats.PreRead.UnixNano())
						if deltaTime <= 0 {
							continue
						}
						cpuCores := deltaCPU / deltaTime

						memoryBytes := stats.MemoryStats.Usage
						inactiveFile, ok := stats.MemoryStats.Stats["inactive_file"]
						if !ok {
							inactiveFile, ok = stats.MemoryStats.Stats["total_inactive_file"]
						}
						if ok {
							if inactiveFile >= memoryBytes {
								memoryBytes = 0
							} else {
								memoryBytes -= inactiveFile
							}
						}
						if memoryBytes > uint64(^uint64(0)>>1) {
							memoryBytes = uint64(^uint64(0) >> 1)
						}
						memory := int64(memoryBytes)
						log.Printf("STATS replica=%s cpu=%.4f cores mem=%d bytes", item.replicaID, cpuCores, memory)
						if err := w.sendMetric(ctx, item.replicaID, cpuCores, memory); err != nil {
							log.Printf("Metrics upload error for replica %s: %v", item.replicaID, err)
						}
					}
				}()
			}
			for _, item := range targets {
				select {
				case jobs <- item:
				case <-ctx.Done():
					close(jobs)
					workers.Wait()
					return
				}
			}
			close(jobs)
			workers.Wait()
		}
	}
}

func (w *WorkerAgent) sendMetric(ctx context.Context, replicaID string, cpuCores float64, memoryBytes int64) error {
	serverURL := w.cfg.Server
	if !strings.HasPrefix(serverURL, "http://") && !strings.HasPrefix(serverURL, "https://") {
		serverURL = "http://" + serverURL
	}
	body, err := json.Marshal(struct {
		ReplicaID   string  `json:"replica_id"`
		CPUCores    float64 `json:"cpu_cores"`
		MemoryBytes int64   `json:"memory_bytes"`
	}{ReplicaID: replicaID, CPUCores: cpuCores, MemoryBytes: memoryBytes})
	if err != nil {
		return fmt.Errorf("encode metric: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, serverURL+"/api/v1/metrics", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create metrics request: %w", err)
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
	if resp.StatusCode == http.StatusNoContent {
		return nil
	}
	responseBody, _ := io.ReadAll(resp.Body)
	return fmt.Errorf("server returned HTTP %d: %s", resp.StatusCode, string(responseBody))
}
