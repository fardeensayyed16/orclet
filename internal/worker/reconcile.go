package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/containerd/errdefs"
	"github.com/fardeensayyed16/orca/pkg/types"
	"github.com/fardeensayyed16/orclet/internal/runtime"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/network"
	"github.com/moby/moby/client"
)

const replicaLabel = "orca.replica-id"

type desiredReplica struct {
	ID                  string              `json:"id"`
	ServiceName         string              `json:"service_name"`
	Image               string              `json:"image"`
	CPUMillis           int64               `json:"cpu_millis"`
	MemoryBytes         int64               `json:"memory_bytes"`
	ContainerPort       int                 `json:"container_port"`
	DesiredRestartCount int                 `json:"desired_restart_count"`
	RestartPolicy       types.RestartPolicy `json:"restart_policy"`
	PullPolicy          types.PullPolicy    `json:"pull_policy"`
	Env                 map[string]string   `json:"env"`
	Command             []string            `json:"command"`
}

type replicaObservation struct {
	ID           string             `json:"id"`
	ContainerID  string             `json:"container_id"`
	HostPort     int                `json:"host_port"`
	Status       types.PodStatus    `json:"status"`
	Health       types.HealthStatus `json:"health"`
	Ready        bool               `json:"ready"`
	RestartCount int                `json:"restart_count"`
	ExitCode     int                `json:"exit_code"`
	Message      string             `json:"message,omitempty"`
	MemoryBytes  int64              `json:"-"`
}

type restartState struct {
	restartCount int
	nextAttempt  time.Time
	delay        time.Duration
}

func (w *WorkerAgent) workLoop(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		if err := w.reconcile(ctx); err != nil && ctx.Err() == nil {
			log.Printf("Work reconciliation error: %v", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (w *WorkerAgent) workURL() string {
	serverURL := w.cfg.Server
	if !strings.HasPrefix(serverURL, "http://") && !strings.HasPrefix(serverURL, "https://") {
		serverURL = "http://" + serverURL
	}
	return strings.TrimRight(serverURL, "/") + "/api/v1/work/" + url.PathEscape(w.cfg.Name)
}

func (w *WorkerAgent) fetchWork(ctx context.Context) ([]desiredReplica, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, w.workURL(), nil)
	if err != nil {
		return nil, err
	}
	if w.cfg.Token != "" {
		req.Header.Set("Authorization", "Bearer "+w.cfg.Token)
	}
	resp, err := w.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, readErr := io.ReadAll(resp.Body)
		if readErr != nil {
			return nil, fmt.Errorf("work endpoint returned HTTP %d (read body: %w)", resp.StatusCode, readErr)
		}
		return nil, fmt.Errorf("work endpoint returned HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	var replicas []desiredReplica
	if err := json.NewDecoder(resp.Body).Decode(&replicas); err != nil {
		return nil, fmt.Errorf("decode desired work: %w", err)
	}
	return replicas, nil
}

func (w *WorkerAgent) reconcile(ctx context.Context) error {
	if w.docker == nil {
		return fmt.Errorf("Docker runtime is not initialized")
	}
	desired, err := w.fetchWork(ctx)
	if err != nil {
		return err
	}
	desiredByID := make(map[string]desiredReplica, len(desired))
	for _, replica := range desired {
		desiredByID[replica.ID] = replica
	}
	w.pruneReplicaState(desiredByID)
	desiredIDs := make(map[string]struct{}, len(desiredByID))
	for id := range desiredByID {
		desiredIDs[id] = struct{}{}
	}

	list, err := w.docker.ContainerList(ctx, client.ContainerListOptions{
		All:     true,
		Filters: make(client.Filters).Add("label", replicaLabel),
	})
	if err != nil {
		return fmt.Errorf("list Orca containers: %w", err)
	}
	for _, summary := range list.Items {
		replicaID := summary.Labels[replicaLabel]
		if replicaID == "" {
			continue
		}
		if _, wanted := desiredByID[replicaID]; !wanted {
			if err := w.removeContainer(ctx, summary.ID); err != nil {
				return fmt.Errorf("remove orphaned container %q: %w", displayContainerName(summary), err)
			}
			if err := w.portPool.release(ctx, replicaID); err != nil {
				return fmt.Errorf("release host port for orphaned replica %q: %w", replicaID, err)
			}
			log.Printf("Removed orphaned container %q", displayContainerName(summary))
		}
	}
	if err := w.portPool.releaseUndesired(ctx, desiredIDs); err != nil {
		return fmt.Errorf("clean stale host-port reservations: %w", err)
	}

	for _, replica := range desired {
		matchesResult, err := w.docker.ContainerList(ctx, client.ContainerListOptions{
			All: true,
			Filters: make(client.Filters).Add(
				"label", replicaLabel+"="+replica.ID,
			),
		})
		if err != nil {
			return fmt.Errorf("find containers for replica %q: %w", replica.ID, err)
		}
		matches := matchesResult.Items
		if len(matches) > 1 {
			for _, extra := range matches[1:] {
				if err := w.removeContainer(ctx, extra.ID); err != nil {
					return fmt.Errorf("remove duplicate container %q: %w", displayContainerName(extra), err)
				}
			}
		}
		if len(matches) == 0 {
			w.createDesired(ctx, replica, replica.DesiredRestartCount)
			continue
		}
		w.reconcileExisting(ctx, replica, matches[0])
	}
	return nil
}

func (w *WorkerAgent) reconcileExisting(ctx context.Context, desired desiredReplica, summary container.Summary) {
	result, err := w.docker.ContainerInspect(ctx, summary.ID, client.ContainerInspectOptions{Size: false})
	if err != nil {
		w.recordFailure(desired, summary.ID, 0, desired.DesiredRestartCount, "inspect failed: "+err.Error())
		log.Printf("Inspect container %q: %v", displayContainerName(summary), err)
		return
	}
	actual := result.Container
	if actual.State == nil || actual.Config == nil {
		w.recordFailure(desired, summary.ID, 0, desired.DesiredRestartCount, "inspect returned missing state or config")
		log.Printf("Inspect container %q returned missing state or config", displayContainerName(summary))
		return
	}
	labels := actual.Config.Labels
	restartCount := desired.DesiredRestartCount
	if rawCount := labels["orca.restart-count"]; rawCount != "" {
		labelledCount, parseErr := strconv.Atoi(rawCount)
		if parseErr != nil || labelledCount < 0 {
			w.recordFailure(desired, summary.ID, 0, restartCount, "invalid orca.restart-count label")
			return
		}
		if labelledCount > restartCount {
			restartCount = labelledCount
		}
	}
	hostPort, _ := strconv.Atoi(labels["orca.host-port"])
	if desired.ContainerPort > 0 && hostPort == 0 {
		if err := w.removeContainer(ctx, summary.ID); err != nil {
			w.recordFailure(desired, summary.ID, 0, restartCount, "remove container for missing host port: "+err.Error())
			return
		}
		w.createDesired(ctx, desired, restartCount)
		return
	}
	if hostPort > 0 {
		if err := w.portPool.reserve(ctx, desired.ID, hostPort); err != nil {
			w.recordFailure(desired, summary.ID, hostPort, restartCount, err.Error())
			return
		}
	}
	if labels["orca.host-ip"] != w.cfg.Address {
		if err := w.removeContainer(ctx, summary.ID); err != nil {
			w.recordFailure(desired, summary.ID, hostPort, restartCount, "remove container after host IP change: "+err.Error())
			return
		}
		w.createDesired(ctx, desired, restartCount)
		return
	}

	state := actual.State
	if state.Status == "exited" || state.Status == "dead" {
		if requiresRestart(desired.RestartPolicy, state.ExitCode) {
			if !w.restartReady(desired.ID, restartCount) {
				w.recordState(desired, summary.ID, hostPort, restartCount, state, "restart backoff")
				return
			}
			if err := w.removeContainer(ctx, summary.ID); err != nil {
				w.recordFailure(desired, summary.ID, hostPort, restartCount, "remove stopped container: "+err.Error())
				return
			}
			nextCount := w.advanceRestart(desired.ID, restartCount+1)
			w.recordObservation(replicaObservation{
				ID: desired.ID, HostPort: hostPort, Status: types.PodStatusStarting,
				Health: types.HealthStatusUnknown, RestartCount: nextCount,
				MemoryBytes: desired.MemoryBytes,
			})
			w.createDesired(ctx, desired, nextCount)
			return
		}
		w.recordState(desired, summary.ID, hostPort, restartCount, state, "")
		return
	}
	if state.Status == "created" {
		if _, err := w.docker.ContainerStart(ctx, summary.ID, client.ContainerStartOptions{}); err != nil {
			w.recordFailure(desired, summary.ID, hostPort, restartCount, "start failed: "+err.Error())
			return
		}
	}
	w.recordState(desired, summary.ID, hostPort, restartCount, state, "")
}

func (w *WorkerAgent) createDesired(ctx context.Context, desired desiredReplica, restartCount int) {
	if err := w.backoffWait(desired.ID); err != nil {
		w.recordObservation(replicaObservation{
			ID: desired.ID, Status: types.PodStatusStarting, Health: types.HealthStatusUnknown,
			RestartCount: restartCount, Message: err.Error(), MemoryBytes: desired.MemoryBytes,
		})
		return
	}
	hostPort := 0
	if desired.ContainerPort > 0 {
		var err error
		hostPort, err = w.portPool.acquire(ctx, desired.ID)
		if err != nil {
			w.recordFailure(desired, "", 0, restartCount, err.Error())
			return
		}
	}
	pullPolicy := desired.PullPolicy
	if pullPolicy == "" {
		pullPolicy = types.PullPolicyIfNotPresent
	}
	switch pullPolicy {
	case types.PullPolicyAlways:
		if err := runtime.PullImage(ctx, w.docker, desired.Image); err != nil {
			w.recordFailure(desired, "", hostPort, restartCount, "pull failed: "+err.Error())
			return
		}
	case types.PullPolicyIfNotPresent:
		_, err := w.docker.ImageInspect(ctx, desired.Image)
		if err != nil {
			if !errdefs.IsNotFound(err) {
				w.recordFailure(desired, "", hostPort, restartCount, "inspect image failed: "+err.Error())
				return
			}
			if err := runtime.PullImage(ctx, w.docker, desired.Image); err != nil {
				w.recordFailure(desired, "", hostPort, restartCount, "pull failed: "+err.Error())
				return
			}
		}
	case types.PullPolicyNever:
		if _, err := w.docker.ImageInspect(ctx, desired.Image); err != nil {
			w.recordFailure(desired, "", hostPort, restartCount, "image not present locally: "+err.Error())
			return
		}
	default:
		w.recordFailure(desired, "", hostPort, restartCount, fmt.Sprintf("unsupported image pull policy %q", pullPolicy))
		return
	}

	cpuMillis := desired.CPUMillis
	if cpuMillis <= 0 {
		cpuMillis = 1000
	}
	memoryBytes := desired.MemoryBytes
	if memoryBytes <= 0 {
		memoryBytes = 512 * 1024 * 1024
	}
	labels := map[string]string{
		"orca.replica-id":    desired.ID,
		"orca.service":       desired.ServiceName,
		"orca.host-ip":       w.cfg.Address,
		"orca.host-port":     strconv.Itoa(hostPort),
		"orca.restart-count": strconv.Itoa(restartCount),
	}
	config := &container.Config{
		Image:        desired.Image,
		Labels:       labels,
		Env:          envList(desired.Env),
		Cmd:          desired.Command,
		ExposedPorts: make(network.PortSet),
	}
	hostConfig := &container.HostConfig{
		Resources: container.Resources{
			NanoCPUs: cpuMillis * 1e6,
			Memory:   memoryBytes,
		},
	}
	if desired.ContainerPort > 0 {
		port, binding, err := runtime.PortBinding(desired.ContainerPort, w.cfg.Address, hostPort)
		if err != nil {
			w.recordFailure(desired, "", hostPort, restartCount, err.Error())
			return
		}
		config.ExposedPorts[port] = struct{}{}
		hostConfig.PortBindings = network.PortMap{port: []network.PortBinding{binding}}
	}
	created, err := w.docker.ContainerCreate(ctx, client.ContainerCreateOptions{
		Config:     config,
		HostConfig: hostConfig,
		Name:       "orca-" + desired.ID,
	})
	if err != nil {
		w.recordFailure(desired, "", hostPort, restartCount, "create failed: "+err.Error())
		return
	}
	if len(created.Warnings) > 0 {
		log.Printf("Docker warnings creating replica %s: %s", desired.ID, strings.Join(created.Warnings, "; "))
	}
	if _, err := w.docker.ContainerStart(ctx, created.ID, client.ContainerStartOptions{}); err != nil {
		w.recordFailure(desired, created.ID, hostPort, restartCount, "start failed: "+err.Error())
		return
	}
	w.recordObservation(replicaObservation{
		ID: desired.ID, ContainerID: created.ID, HostPort: hostPort,
		Status: types.PodStatusStarting, Health: types.HealthStatusUnknown,
		RestartCount: restartCount, MemoryBytes: memoryBytes,
	})
}

func (w *WorkerAgent) removeContainer(ctx context.Context, id string) error {
	_, err := w.docker.ContainerRemove(ctx, id, client.ContainerRemoveOptions{
		Force:         true,
		RemoveVolumes: true,
	})
	return err
}

func (w *WorkerAgent) recordState(desired desiredReplica, id string, hostPort, restartCount int, state *container.State, message string) {
	status := types.PodStatusStarting
	health := types.HealthStatusUnknown
	ready := false
	switch state.Status {
	case "running", "paused":
		status = types.PodStatusRunning
		health = types.HealthStatusHealthy
		ready = true
	case "created", "restarting":
		status = types.PodStatusStarting
	case "exited", "dead":
		if state.ExitCode == 0 {
			status = types.PodStatusCompleted
		} else {
			status = types.PodStatusFailed
		}
	}
	if state.OOMKilled {
		message = fmt.Sprintf("OOMKilled (limit %d Mi)", desired.MemoryBytes/(1024*1024))
	}
	w.recordObservation(replicaObservation{
		ID: desired.ID, ContainerID: id, HostPort: hostPort, Status: status,
		Health: health, Ready: ready, RestartCount: restartCount,
		ExitCode: state.ExitCode, Message: message, MemoryBytes: desired.MemoryBytes,
	})
}

func (w *WorkerAgent) recordFailure(desired desiredReplica, id string, port, restartCount int, message string) {
	w.backoffFailure(desired.ID)
	w.recordObservation(replicaObservation{
		ID: desired.ID, ContainerID: id, HostPort: port, Status: types.PodStatusStarting,
		Health: types.HealthStatusUnknown, RestartCount: restartCount,
		Message: message, MemoryBytes: desired.MemoryBytes,
	})
	log.Printf("Replica %s: %s", desired.ID, message)
}

func requiresRestart(policy types.RestartPolicy, exitCode int) bool {
	switch policy {
	case "", types.RestartPolicyAlways:
		return true
	case types.RestartPolicyOnFailure:
		return exitCode != 0
	default:
		return false
	}
}

func envList(values map[string]string) []string {
	result := make([]string, 0, len(values))
	for key, value := range values {
		result = append(result, key+"="+value)
	}
	return result
}

func displayContainerName(summary container.Summary) string {
	if len(summary.Names) == 0 {
		return summary.ID
	}
	return strings.TrimPrefix(summary.Names[0], "/")
}

func (w *WorkerAgent) recordObservation(observation replicaObservation) {
	w.observationsMu.Lock()
	defer w.observationsMu.Unlock()
	w.observations[observation.ID] = observation
}

func (w *WorkerAgent) observationsSnapshot() []replicaObservation {
	w.observationsMu.RLock()
	defer w.observationsMu.RUnlock()
	result := make([]replicaObservation, 0, len(w.observations))
	for _, observation := range w.observations {
		result = append(result, observation)
	}
	return result
}

func (w *WorkerAgent) pruneReplicaState(desired map[string]desiredReplica) {
	w.observationsMu.Lock()
	for id := range w.observations {
		if _, ok := desired[id]; !ok {
			delete(w.observations, id)
		}
	}
	w.observationsMu.Unlock()
	w.restartMu.Lock()
	for id := range w.restarts {
		if _, ok := desired[id]; !ok {
			delete(w.restarts, id)
		}
	}
	w.restartMu.Unlock()
}

func (w *WorkerAgent) restartReady(id string, count int) bool {
	w.restartMu.Lock()
	defer w.restartMu.Unlock()
	state := w.restarts[id]
	if state.restartCount < count {
		state.restartCount = count
	}
	if state.nextAttempt.IsZero() {
		if state.delay == 0 {
			state.delay = 5 * time.Second
		}
		state.nextAttempt = time.Now().Add(state.delay)
		w.restarts[id] = state
		return false
	}
	if !time.Now().Before(state.nextAttempt) {
		w.restarts[id] = state
		return true
	}
	return false
}

func (w *WorkerAgent) advanceRestart(id string, count int) int {
	w.restartMu.Lock()
	defer w.restartMu.Unlock()
	state := w.restarts[id]
	state.restartCount = count
	if state.delay == 0 {
		state.delay = 5 * time.Second
	} else {
		state.delay *= 2
		if state.delay > 60*time.Second {
			state.delay = 60 * time.Second
		}
	}
	state.nextAttempt = time.Time{}
	w.restarts[id] = state
	return count
}

func (w *WorkerAgent) backoffWait(id string) error {
	w.restartMu.Lock()
	defer w.restartMu.Unlock()
	state := w.restarts[id]
	if !state.nextAttempt.IsZero() && time.Now().Before(state.nextAttempt) {
		return fmt.Errorf("retry backoff until %s", state.nextAttempt.UTC().Format(time.RFC3339))
	}
	return nil
}

func (w *WorkerAgent) backoffFailure(id string) {
	w.restartMu.Lock()
	defer w.restartMu.Unlock()
	state := w.restarts[id]
	if state.delay == 0 {
		state.delay = 5 * time.Second
	} else {
		state.delay *= 2
		if state.delay > 60*time.Second {
			state.delay = 60 * time.Second
		}
	}
	state.nextAttempt = time.Now().Add(state.delay)
	w.restarts[id] = state
}

func resolveNodeAddress(configured, server string) (string, error) {
	if configured != "" {
		address, err := netip.ParseAddr(configured)
		if err != nil {
			return "", fmt.Errorf("invalid worker address %q: %w", configured, err)
		}
		return address.String(), nil
	}
	serverURL := server
	if !strings.Contains(serverURL, "://") {
		serverURL = "http://" + serverURL
	}
	parsed, err := url.Parse(serverURL)
	if err != nil {
		return "", fmt.Errorf("parse server address: %w", err)
	}
	host := parsed.Host
	if _, _, err := net.SplitHostPort(host); err != nil {
		host = net.JoinHostPort(host, "80")
	}
	conn, err := net.Dial("udp", host)
	if err != nil {
		return "", fmt.Errorf("determine worker LAN address: %w", err)
	}
	defer conn.Close()
	udpAddr, ok := conn.LocalAddr().(*net.UDPAddr)
	if !ok {
		return "", fmt.Errorf("unexpected local address type %T", conn.LocalAddr())
	}
	address, ok := netip.AddrFromSlice(udpAddr.IP)
	if !ok {
		return "", fmt.Errorf("invalid local worker address %q", udpAddr.IP)
	}
	return address.Unmap().String(), nil
}

func (w *WorkerAgent) collectReplicaHeartbeats(ctx context.Context) []replicaObservation {
	result := w.observationsSnapshot()
	byID := make(map[string]int, len(result))
	for i := range result {
		byID[result[i].ID] = i
	}
	if w.docker == nil {
		return result
	}
	containers, err := w.docker.ContainerList(ctx, client.ContainerListOptions{
		All:     true,
		Filters: make(client.Filters).Add("label", replicaLabel),
	})
	if err != nil {
		log.Printf("Heartbeat container list error: %v", err)
		return result
	}
	for _, summary := range containers.Items {
		replicaID := summary.Labels[replicaLabel]
		if replicaID == "" {
			continue
		}
		inspected, err := w.docker.ContainerInspect(ctx, summary.ID, client.ContainerInspectOptions{Size: false})
		if err != nil {
			log.Printf("Heartbeat inspect for %q failed: %v", displayContainerName(summary), err)
			continue
		}
		if inspected.Container.State == nil || inspected.Container.Config == nil {
			log.Printf("Heartbeat inspect for %q returned missing state or config", displayContainerName(summary))
			continue
		}
		labels := inspected.Container.Config.Labels
		restartCount, _ := strconv.Atoi(labels["orca.restart-count"])
		hostPort, _ := strconv.Atoi(labels["orca.host-port"])
		observed := replicaObservation{
			ID: replicaID, ContainerID: summary.ID, HostPort: hostPort,
			Status: types.PodStatusStarting, Health: types.HealthStatusUnknown,
			RestartCount: restartCount, MemoryBytes: 0,
		}
		if inspected.Container.HostConfig != nil {
			observed.MemoryBytes = inspected.Container.HostConfig.Resources.Memory
		}
		state := inspected.Container.State
		switch state.Status {
		case "running", "paused":
			observed.Status = types.PodStatusRunning
			observed.Health = types.HealthStatusHealthy
			observed.Ready = true
		case "exited", "dead":
			if state.ExitCode == 0 {
				observed.Status = types.PodStatusCompleted
			} else {
				observed.Status = types.PodStatusFailed
			}
			observed.ExitCode = state.ExitCode
		default:
			observed.Status = types.PodStatusStarting
			observed.Health = types.HealthStatusUnknown
		}
		if previousIndex, ok := byID[replicaID]; ok {
			if observed.MemoryBytes == 0 {
				observed.MemoryBytes = result[previousIndex].MemoryBytes
			}
		}
		if state.OOMKilled {
			observed.Message = fmt.Sprintf("OOMKilled (limit %d Mi)", observed.MemoryBytes/(1024*1024))
		}
		if previousIndex, ok := byID[replicaID]; ok {
			result[previousIndex] = observed
		} else {
			byID[replicaID] = len(result)
			result = append(result, observed)
		}
	}
	return result
}
