package grpc

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"go.unistack.org/micro/v3/broker"
	"go.unistack.org/micro/v3/codec"
	"go.unistack.org/micro/v3/logger"
	"go.unistack.org/micro/v3/metadata"
	"go.unistack.org/micro/v3/options"
	"go.unistack.org/micro/v3/server"
)

var _ server.Message = &rpcMessage{}

type rpcMessage struct {
	payload     interface{}
	codec       codec.Codec
	header      metadata.Metadata
	topic       string
	contentType string
}

func (r *rpcMessage) ContentType() string {
	return r.contentType
}

func (r *rpcMessage) Topic() string {
	return r.topic
}

func (r *rpcMessage) Body() interface{} {
	return r.payload
}

func (r *rpcMessage) Header() metadata.Metadata {
	return r.header
}

func (r *rpcMessage) Codec() codec.Codec {
	return r.codec
}

type handler struct {
	reqType reflect.Type
	ctxType reflect.Type
	method  reflect.Value
}

type subscriber struct {
	topic      string
	rcvr       reflect.Value
	typ        reflect.Type
	subscriber interface{}
	handlers   []*handler
	opts       server.SubscriberOptions
}

func newSubscriber(topic string, sub interface{}, opts ...server.SubscriberOption) server.Subscriber {
	options := server.NewSubscriberOptions(opts...)

	var handlers []*handler

	if typ := reflect.TypeOf(sub); typ.Kind() == reflect.Func {
		h := &handler{
			method: reflect.ValueOf(sub),
		}

		switch typ.NumIn() {
		case 1:
			h.reqType = typ.In(0)
		case 2:
			h.ctxType = typ.In(0)
			h.reqType = typ.In(1)
		}

		handlers = append(handlers, h)

	} else {
		for m := 0; m < typ.NumMethod(); m++ {
			method := typ.Method(m)
			h := &handler{
				method: method.Func,
			}

			switch method.Type.NumIn() {
			case 2:
				h.reqType = method.Type.In(1)
			case 3:
				h.ctxType = method.Type.In(1)
				h.reqType = method.Type.In(2)
			}

			handlers = append(handlers, h)

		}
	}

	return &subscriber{
		rcvr:       reflect.ValueOf(sub),
		typ:        reflect.TypeOf(sub),
		topic:      topic,
		subscriber: sub,
		handlers:   handlers,
		opts:       options,
	}
}

func (g *Server) createSubHandler(sb *subscriber, opts server.Options) broker.Handler {
	return func(p broker.Event) (err error) {
		msg := p.Message()
		// if we don't have headers, create empty map
		if msg.Header == nil {
			msg.Header = make(map[string]string)
		}

		ct := msg.Header["Content-Type"]
		if len(ct) == 0 {
			msg.Header["Content-Type"] = DefaultContentType
			ct = DefaultContentType
		}
		cf, err := g.newCodec(ct)
		if err != nil {
			return err
		}

		hdr := make(map[string]string, len(msg.Header))
		for k, v := range msg.Header {
			hdr[k] = v
		}

		ctx := metadata.NewIncomingContext(sb.opts.Context, hdr)
		ctx = metadata.NewOutgoingContext(ctx, metadata.New(0))

		results := make(chan error, len(sb.handlers))

		for i := 0; i < len(sb.handlers); i++ {
			handler := sb.handlers[i]

			var isVal bool
			var req reflect.Value

			if handler.reqType.Kind() == reflect.Ptr {
				req = reflect.New(handler.reqType.Elem())
			} else {
				req = reflect.New(handler.reqType)
				isVal = true
			}
			if isVal {
				req = req.Elem()
			}

			if err = cf.Unmarshal(msg.Body, req.Interface()); err != nil {
				return err
			}

			fn := func(ctx context.Context, msg server.Message) error {
				var vals []reflect.Value
				if sb.typ.Kind() != reflect.Func {
					vals = append(vals, sb.rcvr)
				}
				if handler.ctxType != nil {
					vals = append(vals, reflect.ValueOf(ctx))
				}

				vals = append(vals, reflect.ValueOf(msg.Body()))

				returnValues := handler.method.Call(vals)
				if rerr := returnValues[0].Interface(); rerr != nil {
					return rerr.(error)
				}
				return nil
			}

			opts.Hooks.EachPrev(func(hook options.Hook) {
				if h, ok := hook.(server.HookSubHandler); ok {
					fn = h(fn)
				}
			})

			if g.wg != nil {
				g.wg.Add(1)
			}
			go func() {
				if g.wg != nil {
					defer g.wg.Done()
				}
				cerr := fn(ctx, &rpcMessage{
					topic:       sb.topic,
					contentType: ct,
					payload:     req.Interface(),
					header:      msg.Header,
				})
				results <- cerr
			}()
		}
		var errors []string
		for i := 0; i < len(sb.handlers); i++ {
			if rerr := <-results; rerr != nil {
				errors = append(errors, rerr.Error())
			}
		}
		if len(errors) > 0 {
			err = fmt.Errorf("subscriber error: %s", strings.Join(errors, "\n"))
		}

		return err
	}
}

func (s *subscriber) Topic() string {
	return s.topic
}

func (s *subscriber) Subscriber() interface{} {
	return s.subscriber
}

func (s *subscriber) Options() server.SubscriberOptions {
	return s.opts
}

func (g *Server) subscribe() error {
	config := g.opts
	subCtx := config.Context

	for sb := range g.subscribers {

		if cx := sb.Options().Context; cx != nil {
			subCtx = cx
		}

		opts := []broker.SubscribeOption{
			broker.SubscribeContext(subCtx),
			broker.SubscribeAutoAck(sb.Options().AutoAck),
			broker.SubscribeBodyOnly(sb.Options().BodyOnly),
		}

		if queue := sb.Options().Queue; len(queue) > 0 {
			opts = append(opts, broker.SubscribeGroup(queue))
		}

		if config.Logger.V(logger.InfoLevel) {
			config.Logger.Info(config.Context, "subscribing to topic: "+sb.Topic())
		}

		sub, err := config.Broker.Subscribe(subCtx, sb.Topic(), g.createSubHandler(sb, config), opts...)
		if err != nil {
			return err
		}

		g.subscribers[sb] = []broker.Subscriber{sub}
	}

	return nil
}
