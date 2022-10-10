package jobs

import (
	"context"
	"github.com/hibiken/asynq"
	endure "github.com/roadrunner-server/endure/pkg/container"
	"go.uber.org/zap"
)

type Middleware interface {
	JobMiddleware(handler asynq.Handler) asynq.Handler
}

func (p *Plugin) AddMiddleware(name endure.Named, middleware Middleware) {
	p.mdwr.Store(name.Name(), asynq.MiddlewareFunc(middleware.JobMiddleware))
}

func (p *Plugin) logMiddleware(handler asynq.Handler) asynq.Handler {
	return asynq.HandlerFunc(func(ctx context.Context, task *asynq.Task) error {
		p.log.Info("handle task", zap.String("task", task.Type()), zap.ByteString("payload", task.Payload()))

		return handler.ProcessTask(ctx, task)
	})
}
