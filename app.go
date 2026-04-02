package rabbitmq_stream

import (
	"bytes"
	"context"
	"fmt"

	"github.com/gogf/gf/v2/encoding/gjson"
	"github.com/gogf/gf/v2/frame/g"
	"github.com/okami-chen/rabbitmq-stream/consts"
	"github.com/pkg/errors"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
)

type ProcessorInterface interface {
	Process(ctx context.Context, j *gjson.Json) error
	Clone() ProcessorInterface
	Pk() string
	Shard() int
	Name() string
	Set(key string, value interface{})
	DefaultVal() int64
}

type ApplicationContext struct {
	AppName     string
	Name        string
	Pk          string
	First       int
	Index       int
	Total       int
	Stop        bool
	Done        chan error
	Ctx         context.Context
	Consumer    *stream.Consumer
	Environment *stream.Environment
	Processor   ProcessorInterface
}

func (c *ApplicationContext) Process(consumerContext stream.ConsumerContext, message *amqp.Message) {

	if c.Stop {
		g.Log().Info(c.Ctx, "consumer context stop, %d", c.Index)
		return
	}

	select {
	case <-c.Ctx.Done():
		c.Stop = true
		g.Log().Info(c.Ctx, "consumer canceled, skipping message")
		return
	default:
	}

	if c.Pk == "" && c.Total > 1 {
		c.Stop = true
		g.Log().Panicf(c.Ctx, "shard enabled and pk is nil")
	}

	j, err := gjson.DecodeToJson(bytes.Join(message.Data, nil))
	if err != nil {
		c.Stop = true
		g.Log().Errorf(c.Ctx, "%s", err)
		c.Done <- errors.Wrap(err, "解析失败")
		return
	}

	pkVal := j.Get("data." + c.Pk).String()
	if c.Total > 1 {
		if mod := HashShardByCount(pkVal, c.Total); mod != c.Index {
			if err = c.Store(c.Ctx, consumerContext.Consumer.GetOffset()); err != nil {
				g.Log().Errorf(c.Ctx, "error %d", c.Index)
				return
			}
			return
		}
	}

	ctx := context.WithValue(c.Ctx, consts.ContextKeyOffSet, consumerContext.Consumer.GetOffset())

	if err = c.Processor.Process(ctx, j); err != nil {
		g.Log().Errorf(c.Ctx, "%s", err)
		c.Stop = true
		c.Done <- errors.Wrap(err, "处理失败")
		return
	}

	if err = c.Store(c.Ctx, consumerContext.Consumer.GetOffset()); err != nil {
		c.Stop = true
		c.Done <- errors.Wrap(err, "保存失败")
		return
	}

	g.Log("bin").Infof(c.Ctx, "%s", j.MustToJson())
}

func (c *ApplicationContext) Clone() *ApplicationContext {
	return &ApplicationContext{
		AppName:   c.AppName,
		Pk:        c.Pk,
		Name:      c.Name,
		First:     1,
		Index:     c.Index,
		Total:     c.Total,
		Stop:      false,
		Done:      make(chan error, 1),
		Ctx:       c.Ctx,
		Consumer:  nil,
		Processor: c.Processor.Clone(),
	}
}

func (c *ApplicationContext) CacheKey() string {
	return fmt.Sprintf("%s:%s:%s:%03d:%03d", c.AppName, c.Name, c.Processor.Name(), c.Total, c.Index)
}

func (c *ApplicationContext) Store(ctx context.Context, value int64) error {
	_, err := g.Redis().Set(ctx, c.CacheKey(), value)
	return err
}
