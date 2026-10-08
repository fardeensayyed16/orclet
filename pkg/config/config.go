package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

type Config struct {
	Server         string `mapstructure:"server"`
	Token          string `mapstructure:"token"`
	Name           string `mapstructure:"name"`
	Address        string `mapstructure:"address"`
	DockerSocket   string `mapstructure:"docker_socket"`
	CapacityCPU    int64  `mapstructure:"capacity_cpu"`
	CapacityMemory int64  `mapstructure:"capacity_memory"`
	HostPortRange  string `mapstructure:"host_port_range"`
}

var GlobalConfig Config

func InitConfig(cmd *cobra.Command) error {
	v := viper.New()

	home, err := os.UserHomeDir()
	if err != nil {
		home = "."
	}

	// Defaults
	v.SetDefault("server", "http://localhost:9001")
	v.SetDefault("token", "")
	v.SetDefault("name", "")
	v.SetDefault("address", "")
	v.SetDefault("docker_socket", "auto")
	v.SetDefault("capacity_cpu", 0)
	v.SetDefault("capacity_memory", 0)
	v.SetDefault("host_port_range", "40000-41000")

	// Config file
	v.SetConfigName("config")
	v.SetConfigType("yaml")
	v.AddConfigPath(filepath.Join(home, ".orclet"))
	v.AddConfigPath("C:\\ProgramData\\orclet")
	v.AddConfigPath(".")

	if err := v.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			return fmt.Errorf("error reading config file: %w", err)
		}
	}

	// Environment variables
	v.SetEnvPrefix("ORCLET")
	v.AutomaticEnv()
	v.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))

	if cmd != nil {
		_ = v.BindPFlags(cmd.Flags())
		_ = v.BindPFlags(cmd.PersistentFlags())
	}

	if err := v.Unmarshal(&GlobalConfig); err != nil {
		return fmt.Errorf("unable to decode into config struct: %w", err)
	}

	return nil
}
