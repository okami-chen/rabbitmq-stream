package rabbitmq_stream

import (
	"context"
	"fmt"
	"time"

	"github.com/gogf/gf/v2/frame/g"
	"github.com/okami-chen/rabbitmq-stream/simple"
)

type RabbitMQConfig struct {
	*RabbitInfo
	Consumers []ConsumersConfig `yaml:"consumers" json:"consumers"`
}

func (r *RabbitMQConfig) Start(ctx context.Context, handles map[string][]ProcessorInterface) {
	for index, consumer := range r.Consumers {
		if consumer.Enable == false {
			g.Log().Warningf(ctx, "igonre stream %s@%s", consumer.Name, consumer.Queue)
			continue
		}
		if objects, ok := handles[consumer.Name]; ok {
			for _, object := range objects {
				instance := object
				simple.SafeGo(ctx, func(ctx context.Context) {
					g.Log().Infof(ctx, "start stream %s@%s", consumer.Name, instance.Name())
					consumer.Run(ctx, r.RabbitInfo, instance, index)
				})
			}
			continue
		}
		g.Log().Warningf(ctx, "ignore stream %s", consumer.Name)
	}
}

type RabbitInfo struct {
	Host     string `yaml:"host" json:"host"`
	Port     int    `yaml:"port" json:"port"`
	Username string `yaml:"username" json:"username"`
	Password string `yaml:"password" json:"password"`
	VHost    string `yaml:"vhost" json:"vhost"`
	Exchange string `yaml:"exchange" json:"exchange"`
}

func (r *RabbitInfo) Address() string {
	return fmt.Sprintf("amqp://%s:%s@%s", r.Username, r.Password, r.Host)
}

func (r *RabbitInfo) Clone() RabbitInfo {
	return RabbitInfo{
		Host:     r.Host,
		Port:     r.Port,
		Username: r.Username,
		Password: r.Password,
		VHost:    r.VHost,
		Exchange: r.Exchange,
	}
}

type ConsumersConfig struct {
	*ConsumerInfo
	//Node []ConsumerNode `yaml:"node" json:"node"`
}

func (r *ConsumersConfig) Run(ctx context.Context, info *RabbitInfo, handle ProcessorInterface, first int) {
	for i := 0; i < r.Num; i++ {
		index := i
		instance := handle.Clone()
		instance.Set("key", fmt.Sprintf("%s:%s:%s:%03d", info.Exchange, r.Name, instance.Name(), index))
		instance.Set("index", index)

		if index == 0 {
			err := NewBind(ctx, info, r.ConsumerInfo)
			if err != nil {
				g.Log().Panic(ctx, err.Error())
			}
			time.Sleep(time.Second * 3)
		}

		simple.SafeGo(ctx, func(ctx context.Context) {
			g.Log().Debugf(ctx, "start -> %s@%s-%03d -> %03d", r.Name, instance.Name(), index, r.Num)
			app := &ApplicationContext{
				AppName:   info.Exchange,
				Name:      r.Name,
				Pk:        handle.Pk(),
				First:     first,
				Index:     index,
				Total:     r.Num,
				Stop:      false,
				Done:      make(chan error, 1),
				Ctx:       ctx,
				Consumer:  nil,
				Processor: instance,
			}
			//g.Log().Infof(ctx, "%+v", app)
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
}

type ConsumerNode struct {
	Name  string `yaml:"name" json:"name"`
	Shard int    `yaml:"shard" json:"shard"`
}
