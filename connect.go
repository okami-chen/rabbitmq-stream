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

func Connect(ctx context.Context, app *ApplicationContext, cfg *RabbitInfo, info *ConsumerInfo) {

	var instance *ApplicationContext

	for {
		if instance != nil {
			if instance.Consumer != nil {
				if err := instance.Consumer.Close(); err != nil {
					g.Log().Warningf(ctx, "failed to close consumer %s@%03d: %v", app.Name, app.Index, err)
				}
				instance.Consumer = nil
			}

			if instance.Environment != nil {
				if err := instance.Environment.Close(); err != nil {
					g.Log().Warningf(ctx, "failed to close environment %s@%03d: %v", app.Name, app.Index, err)
					time.Sleep(time.Second * grand.D(3, 15))
				}
				instance.Environment = nil
			}

			instance = nil
			continue
		}

		instance = app.Clone()
		err := NewInstance(ctx, cfg, info, instance)
		if err != nil {
			g.Log().Warningf(ctx, "%s@%s-%d -> failed to create instance: %v", info.Queue, app.Name, app.Index, err)
			time.Sleep(grand.D(3, 15) * time.Second)
			continue
		}

		select {
		case <-ctx.Done():
			if instance.Consumer != nil {
				if err = instance.Consumer.Close(); err != nil {
					g.Log().Warningf(ctx, "failed to close consumer on context cancel %s@%03d: %v", app.Name, app.Index, err)
				}
			}
			if instance.Environment != nil {
				instance.Environment.Close()
			}
			return

		case err = <-instance.Done:
			if err != nil {
				g.Log().Warningf(ctx, "%s@%d -> consumer error: %v", info.Queue, instance.Index, err)
				if instance.Consumer != nil {
					if err = instance.Consumer.Close(); err != nil {
						g.Log().Warningf(ctx, "failed to close consumer %s@%03d: %v", app.Name, app.Index, err)
					}
				}
				if instance.Environment != nil {
					instance.Environment.Close()
				}
				time.Sleep(time.Second * grand.D(3, 15))
			}
		}
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
					app.Done <- fmt.Errorf("%s@%d environment is nil", info.Queue, app.Index)
					return
				}
				if env.IsClosed() {
					app.Done <- fmt.Errorf("%s@%d environment is closed", info.Queue, app.Index)
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
			app.Done <- fmt.Errorf("%s", errMsg)
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
