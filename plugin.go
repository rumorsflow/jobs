package jobs

import (
	"github.com/go-redis/redis/v8"
	"github.com/hibiken/asynq"
	endure "github.com/roadrunner-server/endure/pkg/container"
	"github.com/roadrunner-server/errors"
	"github.com/rumorsflow/contracts/config"
	rdb "github.com/rumorsflow/contracts/redis"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"strings"
	"sync"
)

const PluginName = "jobs"

type Plugin struct {
	connOpt asynq.RedisConnOpt
	cfg     *Config
	aCfg    *asynq.Config
	server  *asynq.Server
	mux     *asynq.ServeMux
	subMux  *sync.Map
	mdwr    *sync.Map
	once    sync.Once
	log     *zap.Logger
}

func (p *Plugin) Init(cfg config.Configurer, log *zap.Logger, client redis.UniversalClient) error {
	const op = errors.Op("jobs plugin init")

	if !cfg.Has(PluginName) {
		return errors.E(op, errors.Disabled)
	}

	var err error
	if err = cfg.UnmarshalKey(PluginName, &p.cfg); err != nil {
		return errors.E(op, errors.Init, err)
	}

	p.cfg.InitDefault()

	p.aCfg = p.cfg.BuildAsynqConfig()
	p.aCfg.ShutdownTimeout = cfg.GracefulTimeout()
	p.aCfg.Logger = log.Sugar()
	p.aCfg.LogLevel = logLevel(zapcore.LevelOf(log.Core()))
	p.aCfg.GroupAggregator = asynq.GroupAggregatorFunc(p.aggregate)
	p.aCfg.ErrorHandler = asynq.ErrorHandlerFunc(p.errorHandler)

	p.log = log
	p.subMux = new(sync.Map)
	p.mdwr = new(sync.Map)
	p.connOpt = rdb.NewProxy(client)

	p.mux = asynq.NewServeMux()
	p.mux.Use(p.logMiddleware)

	return nil
}

func (p *Plugin) Serve() chan error {
	const op = errors.Op("jobs plugin serve")

	errCh := make(chan error, 1)

	p.once.Do(func() {
		for _, pattern := range p.cfg.Middleware {
			prefix := pattern
			suffix := pattern
			if index := strings.Index(pattern, ":"); index != -1 {
				prefix = pattern[:index+1]
				suffix = pattern[index+1:]
			}
			m, ok := p.mdwr.Load(suffix)
			if !ok {
				if m, ok = p.mdwr.Load(pattern); !ok {
					continue
				}
			}
			if h, ok := p.subMux.Load(prefix); ok {
				h.(*asynq.ServeMux).Use(m.(asynq.MiddlewareFunc))
			} else {
				p.mux.Use(m.(asynq.MiddlewareFunc))
			}
		}
	})

	p.server = asynq.NewServer(p.connOpt, *p.aCfg)
	if err := p.server.Start(p.mux); err != nil {
		errCh <- errors.E(op, errors.Serve, err)
	}

	return errCh
}

func (p *Plugin) Stop() error {
	p.server.Stop()
	p.server.Shutdown()
	return nil
}

// Name returns user-friendly plugin name
func (p *Plugin) Name() string {
	return PluginName
}

func (p *Plugin) Collects() []any {
	return []any{
		p.AddMiddleware,
		p.AddHandler,
	}
}

func (p *Plugin) AddHandler(name endure.Named, handler asynq.Handler) {
	pattern := name.Name()
	if mux, ok := handler.(*asynq.ServeMux); ok {
		pattern = strings.TrimSuffix(pattern, ":") + ":"
		p.subMux.Store(pattern, mux)
		p.mux.Handle(pattern, mux)
	} else if index := strings.Index(pattern, ":"); index != -1 && index+1 != len(pattern) {
		pattern = pattern[:index+1]
		h, ok := p.subMux.LoadOrStore(pattern, asynq.NewServeMux())
		h.(*asynq.ServeMux).Handle(name.Name(), handler)
		if !ok {
			p.mux.Handle(pattern, h.(asynq.Handler))
		}
	} else {
		p.mux.Handle(pattern, handler)
	}
}
