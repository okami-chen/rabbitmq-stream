package rabbitmq_stream

import (
	"context"
	"fmt"
	"time"

	"github.com/gogf/gf/v2/frame/g"
	"github.com/gogf/gf/v2/os/gtime"
	"github.com/gogf/gf/v2/util/grand"
	"github.com/okami-chen/rabbitmq-stream/simple"
	rmq "github.com/rabbitmq/rabbitmq-amqp-go-client/pkg/rabbitmqamqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
)

// Connect 维持一个消费者实例，异常后自动重建，直到 ctx 被取消
func Connect(ctx context.Context, app *ApplicationContext, cfg *RabbitInfo, info *ConsumerInfo) {
	for ctx.Err() == nil {
		instance := app.Clone()
		if err := NewInstance(ctx, cfg, info, instance); err != nil {
			g.Log().Warningf(ctx, "%s@%s-%d -> failed to create instance: %v", info.Queue, app.Name, app.Index, err)
			if !sleepOrDone(ctx, reconnectInterval()) {
				return
			}
			continue
		}

		select {
		case <-ctx.Done():
			instance.close(ctx)
			return
		case err := <-instance.Done:
			g.Log().Warningf(ctx, "%s@%d -> consumer error: %v", info.Queue, instance.Index, err)
			instance.close(ctx)
			if !sleepOrDone(ctx, reconnectInterval()) {
				return
			}
		}
	}
}

// close 释放实例持有的 consumer 与 environment，只关闭一次
func (c *ApplicationContext) close(ctx context.Context) {
	if c.Consumer != nil {
		if err := c.Consumer.Close(); err != nil {
			g.Log().Warningf(ctx, "failed to close consumer %s@%03d: %v", c.Name, c.Index, err)
		}
		c.Consumer = nil
	}
	if c.Environment != nil {
		if err := c.Environment.Close(); err != nil {
			g.Log().Warningf(ctx, "failed to close environment %s@%03d: %v", c.Name, c.Index, err)
		}
		c.Environment = nil
	}
}

func reconnectInterval() time.Duration {
	return grand.D(3, 15) * time.Second
}

// sleepOrDone 等待 duration，ctx 被取消时提前返回 false
func sleepOrDone(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func NewInstance(ctx context.Context, config *RabbitInfo, info *ConsumerInfo, app *ApplicationContext) error {
	env, err := stream.NewEnvironment(
		stream.NewEnvironmentOptions().
			SetHost(config.Host).
			SetPort(config.Port).
			SetUser(config.Username).
			SetPassword(config.Password).
			SetRPCTimeout(time.Second * 10).
			SetAddressResolver(stream.AddressResolver{
				Host: config.Host,
				Port: config.Port,
			}))
	if err != nil {
		return fmt.Errorf("failed to create environment: %w", err)
	}

	val, err := g.Redis().Get(ctx, app.CacheKey())
	if err != nil {
		env.Close()
		return fmt.Errorf("failed to get cache value: %w", err)
	}
	cacheValue := app.Processor.DefaultVal()
	if !val.IsNil() {
		if v := val.Int64(); v != 0 {
			cacheValue = v + 1
		}
	} else {
		if _, err := g.Redis().Set(ctx, app.CacheKey(), cacheValue); err != nil {
			env.Close()
			return fmt.Errorf("failed to set cache value: %w", err)
		}
	}

	simple.SafeGo(ctx, func(ctx context.Context) {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if env == nil {
					app.notifyDone(fmt.Errorf("%s@%d environment is nil", info.Queue, app.Index))
					return
				}
				if env.IsClosed() {
					app.notifyDone(fmt.Errorf("%s@%d environment is closed", info.Queue, app.Index))
					return
				}
			}
		}
	})

	consumer, err := env.NewConsumer(
		info.Queue,
		app.Process,
		stream.NewConsumerOptions().
			SetManualCommit().
			SetClientProvidedName(app.AppName+"-"+app.Processor.Name()).
			SetConsumerName(fmt.Sprintf(
				"%s-%03d@%s",
				app.Processor.Name(), app.Index, gtime.Now().Format("md-Hi")),
			).
			SetOffset(stream.OffsetSpecification{}.Offset(cacheValue)))
	if err != nil {
		env.Close()
		return fmt.Errorf("failed to create consumer: %w", err)
	}

	app.Consumer = consumer
	app.Environment = env

	channelClose := consumer.NotifyClose()
	simple.SafeGo(ctx, func(ctx context.Context) {
		select {
		case event := <-channelClose:
			errMsg := fmt.Sprintf("consumer: %s, reason: %s", event.Name, event.Reason)
			g.Log().Warningf(ctx, "%s@%d consumer closed: %s", info.Queue, app.Index, errMsg)
			env.Close()
			app.notifyDone(fmt.Errorf("%s", errMsg))
		case <-ctx.Done():
			return
		}
	})

	return nil
}

func NewBind(ctx context.Context, config *RabbitInfo, cfg *ConsumerInfo) error {
	env, err := stream.NewEnvironment(
		stream.NewEnvironmentOptions().
			SetHost(config.Host).
			SetPort(config.Port).
			SetUser(config.Username).
			SetPassword(config.Password).
			SetRPCTimeout(time.Second * 10).
			SetAddressResolver(stream.AddressResolver{
				Host: config.Host,
				Port: config.Port,
			}))
	if err != nil {
		return fmt.Errorf("failed to create stream environment: %w", err)
	}
	defer env.Close()

	if exist, e := env.StreamExists(cfg.Queue); e == nil && !exist {
		err = env.DeclareStream(cfg.Queue,
			&stream.StreamOptions{
				MaxLengthBytes: stream.ByteCapacity{}.GB(10),
			})
		if err != nil {
			return fmt.Errorf("failed to declare stream: %w", err)
		}
		g.Log().Infof(ctx, "stream %s created", cfg.Queue)
	} else if e != nil {
		return fmt.Errorf("failed to check stream existence: %w", e)
	}

	environment := rmq.NewEnvironment(config.Address(), nil)
	conn, err := environment.NewConnection(ctx)
	if err != nil {
		return fmt.Errorf("failed to create AMQP connection: %w", err)
	}
	defer func() {
		if err = conn.Close(ctx); err != nil {
			g.Log().Warningf(ctx, "failed to close AMQP connection: %v", err)
		}
	}()

	management := conn.Management()

	for _, routeKey := range cfg.Bind {
		g.Log().Infof(ctx, "binding %s -> %s -> %s", config.Exchange, cfg.Queue, routeKey)
		_, err = management.Bind(ctx, &rmq.ExchangeToQueueBindingSpecification{
			SourceExchange:   config.Exchange,
			DestinationQueue: cfg.Queue,
			BindingKey:       routeKey,
		})
		if err != nil {
			return fmt.Errorf("failed to bind %s -> %s with key %s: %w", config.Exchange, cfg.Queue, routeKey, err)
		}
	}
	return nil
}
