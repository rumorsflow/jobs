package jobs

import (
	"github.com/hibiken/asynq"
	"go.uber.org/zap"
)

func (p *Plugin) aggregate(group string, tasks []*asynq.Task) *asynq.Task {
	p.log.Debug("aggregate", zap.String("group", group))

	payload := []byte{byte(91)} // [
	for i, task := range tasks {
		if i != 0 {
			payload = append(payload, byte(44)) // ,
		}
		payload = append(payload, task.Payload()...)
	} // ,
	payload = append(payload, byte(93)) // ]

	return asynq.NewTask(group, payload, asynq.Queue(AggregateQueue))
}
