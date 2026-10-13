package rabbitmq_stream

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gogf/gf/v2/frame/g"
	"github.com/okami-chen/rabbitmq-stream/simple"
)

type RabbitMQConfig struct {
	*RabbitInfo
	Consumers []ConsumersConfig `yaml:"consumers" json:"consumers"`

	wg sync.WaitGroup // 跟踪Start启动的全部协程，供Wait等待退出
}

// Wait 等待Start启动的协程全部退出（需先取消传给Start的ctx），超时返回false
func (r *RabbitMQConfig) Wait(timeout time.Duration) bool {
	done := make(chan struct{})
	go func() {
		r.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return true
	case <-time.After(timeout):
		return false
	}
}

func (r *RabbitMQConfig) Start(ctx context.Context, handles map[string][]ProcessorInterface) {
	if r == nil || r.RabbitInfo == nil {
		g.Log().Error(ctx, "rabbitmq stream config is empty, nothing to start")
		return
	}
	for index, consumer := range r.Consumers {
		if consumer.ConsumerInfo == nil {
			g.Log().Warning(ctx, "ignore stream consumer with empty config")
			continue
		}
		if !consumer.Enable {
			g.Log().Infof(ctx, "ignore stream %s@%s", consumer.Name, consumer.Queue)
			continue
		}
		objects, ok := handles[consumer.Name]
		if !ok || len(objects) == 0 {
			g.Log().Warningf(ctx, "ignore stream %s, no processor registered", consumer.Name)
			continue
		}
		for _, object := range objects {
			instance := object
			r.wg.Add(1)
			simple.SafeGo(ctx, func(ctx context.Context) {
				defer r.wg.Done()
				g.Log().Infof(ctx, "start stream %s@%s", consumer.Name, instance.Name())
				consumer.run(ctx, r.RabbitInfo, instance, index, &r.wg)
			})
		}
	}
}

type RabbitInfo struct {
	Host     string `yaml:"host" json:"host"`
	Port     int    `yaml:"port" json:"port"` // Stream 协议端口
	Username string `yaml:"username" json:"username"`
	Password string `yaml:"password" json:"password"`
	VHost    string `yaml:"vhost" json:"vhost"`
	Exchange string `yaml:"exchange" json:"exchange"`
	// AmqpPort 声明绑定使用的 AMQP 端口，为 0 时使用 AMQP 默认端口 5672（不是 Stream 端口）
	AmqpPort int `yaml:"amqpPort" json:"amqpPort"`
}

// Address AMQP 连接地址。用户名和密码交给 url 编码，避免 @、/、: 等字符把 URI 解析歪
func (r *RabbitInfo) Address() string {
	return r.buildURL(false)
}

// SafeAddress 隐去密码的连接地址，用于日志输出
func (r *RabbitInfo) SafeAddress() string {
	return r.buildURL(true)
}

func (r *RabbitInfo) buildURL(mask bool) string {
	host := r.Host
	if r.AmqpPort > 0 {
		host = net.JoinHostPort(r.Host, strconv.Itoa(r.AmqpPort))
	}
	u := &url.URL{Scheme: "amqp", Host: host, Path: "/" + strings.TrimPrefix(strings.TrimSpace(r.VHost), "/")}
	if r.Username != "" {
		if mask {
			u.User = url.UserPassword(r.Username, "******")
		} else {
			u.User = url.UserPassword(r.Username, r.Password)
		}
	}
	return u.String()
}

func (r *RabbitInfo) Clone() RabbitInfo {
	return *r
}

type ConsumersConfig struct {
	*ConsumerInfo
}

func (r *ConsumersConfig) Run(ctx context.Context, info *RabbitInfo, handle ProcessorInterface, first int) {
	r.run(ctx, info, handle, first, nil)
}

// run 在wg不为空时登记启动的消费者协程。调用方需已在wg中占位，保证Add发生在Wait归零之前
func (r *ConsumersConfig) run(ctx context.Context, info *RabbitInfo, handle ProcessorInterface, first int, wg *sync.WaitGroup) {
	num := r.Num
	if num <= 0 {
		num = 1
	}
	// 开启分片却没有分片键属于配置错误，启动时直接拒绝，避免在消费回调里 panic
	if num > 1 && handle.Pk() == "" {
		g.Log().Errorf(ctx, "stream %s@%s num=%d but processor %s has no shard key, consumers not started",
			r.Name, r.Queue, num, handle.Name())
		return
	}

	// 声明失败多为 RabbitMQ 暂不可用，按重连间隔重试，直到成功或 ctx 被取消
	for {
		err := NewBind(ctx, info, r.ConsumerInfo)
		if err == nil {
			break
		}
		g.Log().Warningf(ctx, "declare stream %s@%s failed, retrying: %+v", r.Name, r.Queue, err)
		if !sleepOrDone(ctx, reconnectInterval()) {
			return
		}
	}
	if !sleepOrDone(ctx, 3*time.Second) {
		return
	}

	for i := 0; i < num; i++ {
		index := i
		instance := handle.Clone()
		instance.Set("key", fmt.Sprintf("%s:%s:%s:%03d", info.Exchange, r.Name, instance.Name(), index))
		instance.Set("index", index)

		if wg != nil {
			wg.Add(1)
		}
		simple.SafeGo(ctx, func(ctx context.Context) {
			if wg != nil {
				defer wg.Done()
			}
			g.Log().Debugf(ctx, "start -> %s@%s-%03d -> %03d", r.Name, instance.Name(), index, num)
			app := &ApplicationContext{
				AppName:   info.Exchange,
				Name:      r.Name,
				Pk:        handle.Pk(),
				First:     first,
				Index:     index,
				Total:     num,
				LogBody:   r.LogBody,
				Done:      make(chan error, 1),
				Ctx:       ctx,
				Processor: instance,
			}
			Connect(ctx, app, info, r.ConsumerInfo)
		})
	}
}

type ConsumerInfo struct {
	Name   string   `yaml:"name" json:"name"`
	Queue  string   `yaml:"queue" json:"queue"`
	Enable bool     `yaml:"enable" json:"enable"`
	Key    string   `yaml:"key" json:"key"`
	Num    int      `yaml:"num" json:"num"`
	Bind   []string `yaml:"bind" json:"bind"`
	// LogBody 为 true 时把完整消息写入 g.Log("bin")；消息可能含敏感数据，默认只记录大小
	LogBody bool `yaml:"logBody" json:"logBody"`
}

type ConsumerNode struct {
	Name  string `yaml:"name" json:"name"`
	Shard int    `yaml:"shard" json:"shard"`
}
