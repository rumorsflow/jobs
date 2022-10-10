package jobs

import (
	"context"
	"github.com/hibiken/asynq"
	"go.uber.org/zap"
)

func (p *Plugin) errorHandler(_ context.Context, task *asynq.Task, err error) {
	p.log.Error("handle task error", zap.Error(err), zap.String("task", task.Type()), zap.ByteString("payload", task.Payload()))
}
