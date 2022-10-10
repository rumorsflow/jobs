package jobs

import (
	"github.com/hibiken/asynq"
	"go.uber.org/zap/zapcore"
)

func logLevel(l zapcore.Level) asynq.LogLevel {
	switch l {
	case zapcore.DebugLevel:
		return asynq.DebugLevel
	case zapcore.WarnLevel:
		return asynq.WarnLevel
	case zapcore.ErrorLevel:
		return asynq.ErrorLevel
	case zapcore.DPanicLevel, zapcore.PanicLevel, zapcore.FatalLevel:
		return asynq.FatalLevel
	}
	return asynq.InfoLevel
}
