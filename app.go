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
		g.Log().Infof(c.Ctx, "consumer context stop, %d", c.Index)
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
		c.notifyDone(errors.Wrap(err, "解析失败"))
		return
	}

	pkVal := j.Get("after." + c.Pk).String()
	if pkVal == "" {
		pkVal = j.Get("before." + c.Pk).String()
	}
	if pkVal == "" {
		pkVal = j.Get("data." + c.Pk).String()
	}
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
		c.notifyDone(errors.Wrap(err, "处理失败"))
		return
	}

	if err = c.Store(c.Ctx, consumerContext.Consumer.GetOffset()); err != nil {
		c.Stop = true
		c.notifyDone(errors.Wrap(err, "保存失败"))
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

// notifyDone 上报实例异常，通知Connect重建实例。
//
// Done只会被Connect消费一次，之后这个实例就被丢弃了，而消费回调、探活ticker和
// NotifyClose监听是三组互不知情的协程，实例挂掉时往往同时上报。这里必须用非阻塞发送：
// 第一条错误足以触发重连，多余的直接丢弃，否则写满缓冲的那一方会永久阻塞，
// 每次重连都泄漏一个协程以及它持有的env和consumer。
func (c *ApplicationContext) notifyDone(err error) {
	if err == nil || c.Done == nil {
		return
	}
	select {
	case c.Done <- err:
	default:
	}
}

func (c *ApplicationContext) CacheKey() string {
	return fmt.Sprintf("%s:%s:%s:%03d:%03d", c.AppName, c.Name, c.Processor.Name(), c.Total, c.Index)
}

func (c *ApplicationContext) Store(ctx context.Context, value int64) error {
	_, err := g.Redis().Set(ctx, c.CacheKey(), value)
	return err
}
