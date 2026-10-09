package worker

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/fardeensayyed16/orca/pkg/types"
	"github.com/moby/moby/client"
)

type probeState struct {
	livenessFailures int
	livenessSuccess  int
	livenessHealthy  bool
	readinessSuccess int
	readinessReady   bool
	lastLiveness     time.Time
	lastReadiness    time.Time
}

func normalizedProbe(probe *types.Probe, kind string) (*types.Probe, error) {
	if probe == nil {
		return nil, nil
	}
	copy := *probe
	if copy.Kind == "" {
		copy.Kind = kind
	}
	if copy.Kind != kind {
		return nil, fmt.Errorf("probe kind must be %q", kind)
	}
	if copy.PeriodSeconds == 0 {
		copy.PeriodSeconds = 10
	}
	if copy.PeriodSeconds < 1 {
		return nil, errors.New("probe period must be positive")
	}
	if copy.TimeoutSeconds == 0 {
		copy.TimeoutSeconds = 1
	}
	if copy.TimeoutSeconds < 1 {
		return nil, errors.New("probe timeout must be positive")
	}
	if copy.FailureThreshold == 0 {
		copy.FailureThreshold = 3
	}
	if copy.FailureThreshold < 1 {
		return nil, errors.New("probe failure threshold must be positive")
	}
	if copy.SuccessThreshold == 0 {
		copy.SuccessThreshold = 1
	}
	if copy.SuccessThreshold < 1 {
		return nil, errors.New("probe success threshold must be positive")
	}
	if copy.InitialDelaySeconds < 0 {
		return nil, errors.New("probe initial delay cannot be negative")
	}
	switch copy.Type {
	case "http":
		if copy.Path == "" {
			copy.Path = "/"
		}
		if !strings.HasPrefix(copy.Path, "/") {
			return nil, errors.New("HTTP probe path must begin with '/'")
		}
		if copy.Port < 1 || copy.Port > 65535 {
			return nil, errors.New("HTTP probe port must be between 1 and 65535")
		}
	case "tcp":
		if copy.Port < 1 || copy.Port > 65535 {
			return nil, errors.New("TCP probe port must be between 1 and 65535")
		}
	case "exec":
		if len(copy.Command) == 0 || strings.TrimSpace(copy.Command[0]) == "" {
			return nil, errors.New("exec probe requires a command")
		}
	default:
		return nil, errors.New("probe type must be http, tcp, or exec")
	}
	return &copy, nil
}

func (w *WorkerAgent) healthLoop(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			w.reconcileMu.Lock()
			w.checkHealth(ctx)
			w.reconcileMu.Unlock()
		}
	}
}

func (w *WorkerAgent) checkHealth(ctx context.Context) {
	desired := w.desiredSnapshot()
	for id, replica := range desired {
		observation, ok := w.observation(id)
		if !ok || observation.ContainerID == "" || observation.Status != types.PodStatusRunning {
			continue
		}
		inspected, err := w.docker.ContainerInspect(ctx, observation.ContainerID, client.ContainerInspectOptions{Size: false})
		if err != nil {
			w.recordProbeError(id, "inspect before health check failed: "+err.Error())
			continue
		}
		if inspected.Container.State == nil || inspected.Container.Config == nil {
			w.recordProbeError(id, "inspect before health check returned missing state or config")
			continue
		}
		state := inspected.Container.State
		if state.Status != "running" && state.Status != "paused" {
			continue
		}
		startedAt, err := time.Parse(time.RFC3339Nano, state.StartedAt)
		if err != nil {
			w.recordProbeError(id, "parse container start time: "+err.Error())
			continue
		}

		liveness, err := normalizedProbe(replica.Liveness, "liveness")
		if err != nil {
			w.markProbeConfigurationFailure(replica, observation, err)
			continue
		}
		readiness, err := normalizedProbe(replica.Readiness, "readiness")
		if err != nil {
			w.markProbeConfigurationFailure(replica, observation, err)
			continue
		}
		probeStatus := w.probeState(id)
		now := time.Now()
		if liveness != nil && probeDue(now, startedAt, probeStatus.lastLiveness, liveness) {
			probeStatus.lastLiveness = now
			probeCtx, cancel := context.WithTimeout(ctx, time.Duration(liveness.TimeoutSeconds)*time.Second)
			success, probeErr := w.runProbe(probeCtx, observation, liveness)
			cancel()
			if probeErr != nil {
				success = false
				w.recordProbeError(id, "liveness probe: "+probeErr.Error())
			}
			if success {
				probeStatus.livenessFailures = 0
				probeStatus.livenessSuccess++
				if probeStatus.livenessSuccess >= liveness.SuccessThreshold {
					probeStatus.livenessHealthy = true
				}
			} else {
				probeStatus.livenessSuccess = 0
				probeStatus.livenessFailures++
			}
		}
		if readiness != nil && probeDue(now, startedAt, probeStatus.lastReadiness, readiness) {
			probeStatus.lastReadiness = now
			probeCtx, cancel := context.WithTimeout(ctx, time.Duration(readiness.TimeoutSeconds)*time.Second)
			success, probeErr := w.runProbe(probeCtx, observation, readiness)
			cancel()
			if probeErr != nil {
				success = false
				w.recordProbeError(id, "readiness probe: "+probeErr.Error())
			}
			if success {
				probeStatus.readinessSuccess++
				if probeStatus.readinessSuccess >= readiness.SuccessThreshold {
					probeStatus.readinessReady = true
				}
			} else {
				probeStatus.readinessSuccess = 0
				probeStatus.readinessReady = false
			}
		}
		w.setProbeState(id, probeStatus)

		health := types.HealthStatusHealthy
		if liveness != nil {
			health = types.HealthStatusUnknown
			if probeStatus.livenessHealthy {
				health = types.HealthStatusHealthy
			}
			if probeStatus.livenessFailures >= liveness.FailureThreshold {
				health = types.HealthStatusUnhealthy
			}
		}
		ready := readiness == nil || probeStatus.readinessReady
		if health == types.HealthStatusUnhealthy {
			ready = false
		}
		observation.Health = health
		observation.Ready = ready
		observation.Message = ""
		w.recordObservation(observation)

		if liveness != nil && probeStatus.livenessFailures >= liveness.FailureThreshold {
			w.handleLivenessFailure(ctx, replica, observation)
		}
	}
}

func probeDue(now, startedAt, lastProbe time.Time, probe *types.Probe) bool {
	if now.Before(startedAt.Add(time.Duration(probe.InitialDelaySeconds) * time.Second)) {
		return false
	}
	if lastProbe.IsZero() {
		return true
	}
	return !now.Before(lastProbe.Add(time.Duration(probe.PeriodSeconds) * time.Second))
}

func (w *WorkerAgent) runProbe(ctx context.Context, observation replicaObservation, probe *types.Probe) (bool, error) {
	switch probe.Type {
	case "http":
		return runHTTPProbe(ctx, w.cfg.Address, observation.HostPort, probe.Path)
	case "tcp":
		return runTCPProbe(ctx, w.cfg.Address, observation.HostPort)
	case "exec":
		return w.runExecProbe(ctx, observation.ContainerID, probe.Command)
	default:
		return false, fmt.Errorf("unsupported probe type %q", probe.Type)
	}
}

func runHTTPProbe(ctx context.Context, host string, hostPort int, path string) (bool, error) {
	if hostPort <= 0 {
		return false, errors.New("HTTP probe requires a published host port")
	}
	target := "http://" + net.JoinHostPort(host, strconv.Itoa(hostPort)) + path
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return false, err
	}
	httpClient := &http.Client{
		Timeout: timeoutFromContext(ctx),
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	response, err := httpClient.Do(request)
	if err != nil {
		return false, err
	}
	defer response.Body.Close()
	return response.StatusCode >= http.StatusOK && response.StatusCode <= 399, nil
}

func runTCPProbe(ctx context.Context, host string, hostPort int) (bool, error) {
	if hostPort <= 0 {
		return false, errors.New("TCP probe requires a published host port")
	}
	connection, err := (&net.Dialer{}).DialContext(ctx, "tcp", net.JoinHostPort(host, strconv.Itoa(hostPort)))
	if err != nil {
		return false, err
	}
	return true, connection.Close()
}

func timeoutFromContext(ctx context.Context) time.Duration {
	deadline, ok := ctx.Deadline()
	if !ok {
		return time.Second
	}
	timeout := time.Until(deadline)
	if timeout <= 0 {
		return time.Nanosecond
	}
	return timeout
}

func (w *WorkerAgent) runExecProbe(ctx context.Context, containerID string, command []string) (bool, error) {
	created, err := w.docker.ExecCreate(ctx, containerID, client.ExecCreateOptions{Cmd: command})
	if err != nil {
		return false, fmt.Errorf("create Docker exec: %w", err)
	}
	if _, err := w.docker.ExecStart(ctx, created.ID, client.ExecStartOptions{Detach: true}); err != nil {
		return false, fmt.Errorf("start Docker exec: %w", err)
	}
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		result, err := w.docker.ExecInspect(ctx, created.ID, client.ExecInspectOptions{})
		if err != nil {
			return false, fmt.Errorf("inspect Docker exec: %w", err)
		}
		if !result.Running {
			return result.ExitCode == 0, nil
		}
		select {
		case <-ctx.Done():
			return false, fmt.Errorf("exec probe timed out: %w", ctx.Err())
		case <-ticker.C:
		}
	}
}

func (w *WorkerAgent) handleLivenessFailure(ctx context.Context, replica desiredReplica, observation replicaObservation) {
	if replica.RestartPolicy == types.RestartPolicyNever {
		if observation.ContainerID != "" {
			if err := w.removeContainer(ctx, observation.ContainerID); err != nil {
				observation.Message = "liveness probe failed; container removal failed: " + err.Error()
			} else {
				observation.ContainerID = ""
			}
		}
		observation.Status = types.PodStatusFailed
		observation.Health = types.HealthStatusUnhealthy
		observation.Ready = false
		if observation.Message == "" {
			observation.Message = "liveness probe failed"
		}
		w.recordObservation(observation)
		w.setProbeState(replica.ID, probeState{})
		return
	}
	if !requiresRestart(replica.RestartPolicy, 1) {
		return
	}
	if !w.restartReady(replica.ID, observation.RestartCount) {
		return
	}
	if err := w.removeContainer(ctx, observation.ContainerID); err != nil {
		w.recordProbeError(replica.ID, "remove container after liveness failure: "+err.Error())
		return
	}
	nextCount := w.advanceRestart(replica.ID, observation.RestartCount+1)
	w.setProbeState(replica.ID, probeState{})
	observation.ContainerID = ""
	observation.Status = types.PodStatusStarting
	observation.Health = types.HealthStatusUnknown
	observation.Ready = false
	observation.RestartCount = nextCount
	observation.Message = "restarting after liveness failure"
	w.recordObservation(observation)
	w.createDesired(ctx, replica, nextCount)
}

func (w *WorkerAgent) markProbeConfigurationFailure(_ desiredReplica, observation replicaObservation, err error) {
	observation.Health = types.HealthStatusUnhealthy
	observation.Ready = false
	observation.Message = "invalid probe configuration: " + err.Error()
	w.recordObservation(observation)
}

func (w *WorkerAgent) recordProbeError(id, message string) {
	observation, ok := w.observation(id)
	if !ok {
		return
	}
	observation.Message = message
	w.recordObservation(observation)
}

func (w *WorkerAgent) probeState(id string) probeState {
	w.probeMu.Lock()
	defer w.probeMu.Unlock()
	return w.probes[id]
}

func (w *WorkerAgent) setProbeState(id string, state probeState) {
	w.probeMu.Lock()
	defer w.probeMu.Unlock()
	w.probes[id] = state
}

func (w *WorkerAgent) observation(id string) (replicaObservation, bool) {
	w.observationsMu.RLock()
	defer w.observationsMu.RUnlock()
	observation, ok := w.observations[id]
	return observation, ok
}

func (w *WorkerAgent) desiredSnapshot() map[string]desiredReplica {
	w.desiredMu.RLock()
	defer w.desiredMu.RUnlock()
	result := make(map[string]desiredReplica, len(w.desired))
	for id, replica := range w.desired {
		result[id] = replica
	}
	return result
}

func (w *WorkerAgent) setDesired(replicas []desiredReplica) {
	w.desiredMu.Lock()
	defer w.desiredMu.Unlock()
	w.desired = make(map[string]desiredReplica, len(replicas))
	for _, replica := range replicas {
		w.desired[replica.ID] = replica
	}
}
