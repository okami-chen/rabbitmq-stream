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

	//if app.First == 0 {
	//	NewBind(ctx, cfg, info)
	//}

	for {
		if instance != nil && instance.Consumer != nil {
			if err := instance.Consumer.Close(); err != nil {
				instance = nil
				g.Log().Infof(ctx, "failed to close %s@%03d %v", app.Name, app.Index, err)
				time.Sleep(time.Second * grand.D(3, 15))
				continue
			}
		}

		instance = app.Clone()
		err := NewInstance(ctx, cfg, info, instance)
		if err != nil {
			g.Log().Warningf(ctx, "%s@%s-%d -> %v", info.Queue, app.Name, app.Index, err)
			time.Sleep(grand.D(3, 15) * time.Second)
			continue
		}

		// 阻塞等待错误或上下文取消
		select {
		case <-ctx.Done():
			// 上下文取消，安全关闭实例
			if err = instance.Consumer.Close(); err != nil {
				instance = nil
				g.Log().Infof(ctx, "failed to cancelled %s@%03d %v", app.Name, app.Index, err)
			}
			return

		case err = <-instance.Done:
			if err != nil {
				g.Log().Warningf(ctx, "%s@%d -> %v", info.Queue, instance.Index, err)
				if err = instance.Consumer.Close(); err != nil {
					instance = nil
					g.Log().Infof(ctx, "failed to close %s@%03d %v", app.Name, app.Index, err)
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
		g.Log().Warningf(ctx, "error open stream %s", err.Error())
		return err
	}
	// 不存在才定义
	if exist, err := env.StreamExists(info.Queue); err == nil && !exist {
		err = env.DeclareStream(info.Queue,
			&stream.StreamOptions{
				MaxLengthBytes: stream.ByteCapacity{}.GB(10),
			})
		if err != nil {
			g.Log().Warningf(ctx, "error declaring stream %s", err.Error())
			return err
		}
	}

	val, err := g.Redis().Get(ctx, app.CacheKey())
	if err != nil {
		return err
	}
	cacheValue := app.Processor.DefaultVal() // 默认值
	if !val.IsNil() {
		if v := val.Int64(); v != 0 {
			cacheValue = v
		}
	} else {
		g.Redis().Set(ctx, app.CacheKey(), cacheValue)
	}

	//g.Log().Infof(ctx, "value -> %s -> %d", app.Processor.Key(), cacheValue)

	simple.SafeGo(ctx, func(ctx context.Context) {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if env == nil {
					app.Done <- fmt.Errorf("%s@%d  env is nil", info.Queue, app.Index)
					return
				}
				if env.IsClosed() {
					app.Done <- fmt.Errorf("%s@%d  env is closed", info.Queue, app.Index)
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
			SetConsumerName(fmt.Sprintf(
				"%s-%03d@%s",
				app.Processor.Name(), app.Index, gtime.Now().Format("md-Hi")),
			).
			SetOffset(stream.OffsetSpecification{}.Offset(cacheValue)))
	if err != nil {
		g.Log().Warningf(ctx, "error declaring consumer %s", err.Error())
		env.Close()
		return err
	}

	app.Consumer = consumer

	channelClose := consumer.NotifyClose()
	simple.SafeGo(ctx, func(ctx context.Context) {
		select {
		case event := <-channelClose:
			er := fmt.Sprintf("consumer: %s , reason: %s", event.Name, event.Reason)
			env.Close()
			app.Done <- fmt.Errorf(er)
		case <-ctx.Done():
			return
		}
	})

	return nil
}

func NewBind(ctx context.Context, config *RabbitInfo, cfg *ConsumerInfo) {
	env := rmq.NewEnvironment(config.Address(), nil)
	conn, err := env.NewConnection(ctx)
	if err != nil {
		rmq.Error("Error opening connection", err)
		return
	}
	defer conn.Close(ctx)

	management := conn.Management()

	for _, routeKey := range cfg.Bind {
		g.Log().Infof(ctx, "bind %s -> %s -> %s", config.Exchange, cfg.Queue, routeKey)
		_, err = management.Bind(ctx, &rmq.ExchangeToQueueBindingSpecification{
			SourceExchange:   config.Exchange,
			DestinationQueue: cfg.Queue,
			BindingKey:       routeKey,
		})
	}
}
