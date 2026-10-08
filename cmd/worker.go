package cmd

import (
	"fmt"

	"github.com/fardeensayyed16/orclet/pkg/config"
	"github.com/spf13/cobra"
)

var (
	workerConfig         string
	workerServer         string
	workerToken          string
	workerName           string
	workerAddress        string
	workerDockerSocket   string
	workerCapacityCPU    int64
	workerCapacityMemory int64
	workerHostPortRange  string
)

var workerCmd = &cobra.Command{
	Use:   "worker [flags]",
	Short: "Start the Orclet worker node agent",
	RunE: func(cmd *cobra.Command, args []string) error {
		fmt.Printf("Starting Orclet worker node... Connecting to server %s\n", config.GlobalConfig.Server)
		return nil
	},
}

func init() {
	workerCmd.Flags().StringVar(&workerConfig, "config", "", "Path to config file")
	workerCmd.Flags().StringVar(&workerServer, "server", "", "Orca master server URL (e.g. 192.168.1.10:9001)")
	workerCmd.Flags().StringVar(&workerToken, "token", "", "Worker authentication token")
	workerCmd.Flags().StringVar(&workerName, "name", "", "Unique worker node name")
	workerCmd.Flags().StringVar(&workerAddress, "address", "", "Worker LAN IP address")
	workerCmd.Flags().StringVar(&workerDockerSocket, "docker-socket", "auto", "Docker socket path or connection string")
	workerCmd.Flags().Int64Var(&workerCapacityCPU, "capacity-cpu", 0, "Override reported CPU capacity (millicores)")
	workerCmd.Flags().Int64Var(&workerCapacityMemory, "capacity-memory", 0, "Override reported memory capacity (bytes)")
	workerCmd.Flags().StringVar(&workerHostPortRange, "host-port-range", "40000-41000", "Host port pool range for published ports")

	rootCmd.AddCommand(workerCmd)
}
