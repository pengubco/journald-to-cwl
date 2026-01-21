package config

import (
	"fmt"

	"github.com/spf13/viper"
)

const (
	DefaultLogGroup  = "journal-logs"
	DefaultStateFile = "/var/lib/journald-to-cwl/state"
)

type Config struct {
	LogGroup string `mapstructure:"log_group"`

	LogStream string `mapstructure:"log_stream"`

	StateFile string `mapstructure:"state_file"`

	SkipAuditLog bool `mapstructure:"skip_audit_log"`
}

func InitalizeConfig(instanceID string, args []string) (*Config, error) {
	var c Config
	v := viper.New()
	v.SetDefault("log_group", DefaultLogGroup)
	v.SetDefault("state_file", DefaultStateFile)
	v.SetDefault("skip_audit_log", false)
	if len(args) >= 1 {
		configFile := args[0]
		v.SetConfigType("env")
		v.SetConfigFile(configFile)
		if err := v.ReadInConfig(); err != nil {
			return nil, fmt.Errorf("cannot read config from %s, %w", configFile, err)
		}
	}
	if err := v.Unmarshal(&c); err != nil {
		return nil, fmt.Errorf("cannot unmarshal config, %w", err)
	}
	if c.LogStream == "" {
		c.LogStream = instanceID
	}
	return &c, nil
}
