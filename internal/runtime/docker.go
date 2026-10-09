package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"net/netip"
	"strconv"

	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/network"
	"github.com/moby/moby/client"
)

type DockerRuntime interface {
	Info(context.Context, client.InfoOptions) (client.SystemInfoResult, error)
	ImageInspect(context.Context, string, ...client.ImageInspectOption) (client.ImageInspectResult, error)
	ImagePull(context.Context, string, client.ImagePullOptions) (client.ImagePullResponse, error)
	ContainerCreate(context.Context, client.ContainerCreateOptions) (client.ContainerCreateResult, error)
	ContainerStart(context.Context, string, client.ContainerStartOptions) (client.ContainerStartResult, error)
	ContainerStop(context.Context, string, client.ContainerStopOptions) (client.ContainerStopResult, error)
	ContainerRemove(context.Context, string, client.ContainerRemoveOptions) (client.ContainerRemoveResult, error)
	ContainerList(context.Context, client.ContainerListOptions) (client.ContainerListResult, error)
	ContainerInspect(context.Context, string, client.ContainerInspectOptions) (client.ContainerInspectResult, error)
	ContainerStats(context.Context, string, client.ContainerStatsOptions) (client.ContainerStatsResult, error)
	ExecCreate(context.Context, string, client.ExecCreateOptions) (client.ExecCreateResult, error)
	ExecStart(context.Context, string, client.ExecStartOptions) (client.ExecStartResult, error)
	ExecInspect(context.Context, string, client.ExecInspectOptions) (client.ExecInspectResult, error)
}

type dockerRuntime struct {
	client *client.Client
}

func NewDockerRuntime(dockerHost string) (DockerRuntime, error) {
	var (
		cli *client.Client
		err error
	)
	if dockerHost == "" || dockerHost == "auto" {
		cli, err = client.New(client.FromEnv)
	} else {
		cli, err = client.New(client.FromEnv, client.WithHost(dockerHost))
	}
	if err != nil {
		return nil, fmt.Errorf("create Docker client: %w", err)
	}
	return &dockerRuntime{client: cli}, nil
}

func (d *dockerRuntime) Info(ctx context.Context, options client.InfoOptions) (client.SystemInfoResult, error) {
	return d.client.Info(ctx, options)
}

func (d *dockerRuntime) ImageInspect(ctx context.Context, ref string, options ...client.ImageInspectOption) (client.ImageInspectResult, error) {
	return d.client.ImageInspect(ctx, ref, options...)
}

func (d *dockerRuntime) ImagePull(ctx context.Context, ref string, options client.ImagePullOptions) (client.ImagePullResponse, error) {
	return d.client.ImagePull(ctx, ref, options)
}

func (d *dockerRuntime) ContainerCreate(ctx context.Context, options client.ContainerCreateOptions) (client.ContainerCreateResult, error) {
	return d.client.ContainerCreate(ctx, options)
}

func (d *dockerRuntime) ContainerStart(ctx context.Context, id string, options client.ContainerStartOptions) (client.ContainerStartResult, error) {
	return d.client.ContainerStart(ctx, id, options)
}

func (d *dockerRuntime) ContainerStop(ctx context.Context, id string, options client.ContainerStopOptions) (client.ContainerStopResult, error) {
	return d.client.ContainerStop(ctx, id, options)
}

func (d *dockerRuntime) ContainerRemove(ctx context.Context, id string, options client.ContainerRemoveOptions) (client.ContainerRemoveResult, error) {
	return d.client.ContainerRemove(ctx, id, options)
}

func (d *dockerRuntime) ContainerList(ctx context.Context, options client.ContainerListOptions) (client.ContainerListResult, error) {
	return d.client.ContainerList(ctx, options)
}

func (d *dockerRuntime) ContainerInspect(ctx context.Context, id string, options client.ContainerInspectOptions) (client.ContainerInspectResult, error) {
	return d.client.ContainerInspect(ctx, id, options)
}

func (d *dockerRuntime) ContainerStats(ctx context.Context, id string, options client.ContainerStatsOptions) (client.ContainerStatsResult, error) {
	return d.client.ContainerStats(ctx, id, options)
}

func (d *dockerRuntime) ExecCreate(ctx context.Context, id string, options client.ExecCreateOptions) (client.ExecCreateResult, error) {
	return d.client.ExecCreate(ctx, id, options)
}

func (d *dockerRuntime) ExecStart(ctx context.Context, id string, options client.ExecStartOptions) (client.ExecStartResult, error) {
	return d.client.ExecStart(ctx, id, options)
}

func (d *dockerRuntime) ExecInspect(ctx context.Context, id string, options client.ExecInspectOptions) (client.ExecInspectResult, error) {
	return d.client.ExecInspect(ctx, id, options)
}

func PullImage(ctx context.Context, docker DockerRuntime, ref string) error {
	response, err := docker.ImagePull(ctx, ref, client.ImagePullOptions{})
	if err != nil {
		return err
	}
	defer response.Close()
	if err := response.Wait(ctx); err != nil {
		return err
	}
	return nil
}

func ReadStats(ctx context.Context, docker DockerRuntime, id string) (container.StatsResponse, error) {
	response, err := docker.ContainerStats(ctx, id, client.ContainerStatsOptions{
		Stream:                false,
		IncludePreviousSample: true,
	})
	if err != nil {
		return container.StatsResponse{}, err
	}
	defer response.Body.Close()
	var stats container.StatsResponse
	if err := json.NewDecoder(response.Body).Decode(&stats); err != nil {
		return container.StatsResponse{}, fmt.Errorf("decode Docker stats: %w", err)
	}
	return stats, nil
}

func PortBinding(port int, hostIP string, hostPort int) (network.Port, network.PortBinding, error) {
	dockerPort, err := network.ParsePort(strconv.Itoa(port) + "/tcp")
	if err != nil {
		return network.Port{}, network.PortBinding{}, fmt.Errorf("parse container port: %w", err)
	}
	address, err := netip.ParseAddr(hostIP)
	if err != nil {
		return network.Port{}, network.PortBinding{}, fmt.Errorf("parse node address: %w", err)
	}
	return dockerPort, network.PortBinding{HostIP: address, HostPort: strconv.Itoa(hostPort)}, nil
}
