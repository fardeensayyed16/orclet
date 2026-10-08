package cmd

import (
	"fmt"
	"os"

	"github.com/fardeensayyed16/orclet/pkg/config"
	"github.com/spf13/cobra"
)

var (
	serverFlag string
	tokenFlag  string
)

var rootCmd = &cobra.Command{
	Use:   "orclet",
	Short: "Orclet Worker Node CLI / Agent",
	Long:  `Orclet is the node worker agent for Orca container orchestrator.`,
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		return config.InitConfig(cmd)
	},
	RunE: func(cmd *cobra.Command, args []string) error {
		return cmd.Help()
	},
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func init() {
	rootCmd.PersistentFlags().StringVar(&serverFlag, "server", "", "Orca master server URL")
	rootCmd.PersistentFlags().StringVar(&tokenFlag, "token", "", "Worker authentication token")
}
