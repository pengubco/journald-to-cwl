package config

import (
	"fmt"
	"regexp"
	"strings"

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

	// IgnorePatterns contains the regex pattern strings from config
	IgnorePatterns []string `mapstructure:"ignore_patterns"`

	// compiledIgnorePatterns stores compiled regex patterns for efficient matching
	compiledIgnorePatterns []*regexp.Regexp
}

func InitalizeConfig(instanceID string, args []string) (*Config, error) {
	var c Config
	v := viper.New()
	v.SetDefault("log_group", DefaultLogGroup)
	v.SetDefault("state_file", DefaultStateFile)
	v.SetDefault("skip_audit_log", false)
	if len(args) >= 1 {
		configFile := args[0]
		v.SetConfigType("toml")
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

	// Compile regex patterns
	if len(c.IgnorePatterns) > 0 {
		compiled, err := compilePatterns(c.IgnorePatterns)
		if err != nil {
			return nil, fmt.Errorf("invalid ignore patterns: %w", err)
		}
		c.compiledIgnorePatterns = compiled
	}

	return &c, nil
}

// compilePatterns compiles regex patterns and returns error on first failure.
// Empty or whitespace-only patterns are skipped.
func compilePatterns(patterns []string) ([]*regexp.Regexp, error) {
	compiled := make([]*regexp.Regexp, 0, len(patterns))
	for i, pattern := range patterns {
		// Skip empty or whitespace-only patterns
		if strings.TrimSpace(pattern) == "" {
			continue
		}
		re, err := regexp.Compile(pattern)
		if err != nil {
			return nil, fmt.Errorf("pattern %d (%q): %w", i, pattern, err)
		}
		compiled = append(compiled, re)
	}
	return compiled, nil
}

// ShouldFilterMessage returns true if the message matches any ignore pattern.
// Returns false if no patterns are configured or if no patterns match.
func (c *Config) ShouldFilterMessage(message string) bool {
	for _, re := range c.compiledIgnorePatterns {
		if re.MatchString(message) {
			return true
		}
	}
	return false
}
