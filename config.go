package jobs

import (
	"github.com/hibiken/asynq"
	"time"
)

const (
	DefaultQueue   = "default"
	AggregateQueue = "aggregate"
)

type Config struct {
	// List of the middleware names (order will be preserved)
	Middleware               []string       `mapstructure:"middleware"`
	Concurrency              int            `mapstructure:"concurrency"`
	Queues                   map[string]int `mapstructure:"queues"`
	StrictPriority           bool           `mapstructure:"strict_priority"`
	HealthCheckInterval      time.Duration  `mapstructure:"health_check_interval"`
	DelayedTaskCheckInterval time.Duration  `mapstructure:"delayed_task_check_interval"`
	GroupGracePeriod         time.Duration  `mapstructure:"group_grace_period"`
	GroupMaxDelay            time.Duration  `mapstructure:"group_max_delay"`
	GroupMaxSize             int            `mapstructure:"group_max_size"`
}

func (cfg *Config) InitDefault() {
	if cfg.Queues == nil {
		cfg.Queues = make(map[string]int)
	}

	if _, ok := cfg.Queues[DefaultQueue]; !ok {
		cfg.Queues[DefaultQueue] = 1
	}

	if _, ok := cfg.Queues[AggregateQueue]; !ok {
		cfg.Queues[AggregateQueue] = 5
	}
}

func (cfg *Config) BuildAsynqConfig() *asynq.Config {
	return &asynq.Config{
		Concurrency:              cfg.Concurrency,
		Queues:                   cfg.Queues,
		StrictPriority:           cfg.StrictPriority,
		HealthCheckInterval:      cfg.HealthCheckInterval,
		DelayedTaskCheckInterval: cfg.DelayedTaskCheckInterval,
		GroupGracePeriod:         cfg.GroupGracePeriod,
		GroupMaxDelay:            cfg.GroupMaxDelay,
		GroupMaxSize:             cfg.GroupMaxSize,
	}
}
