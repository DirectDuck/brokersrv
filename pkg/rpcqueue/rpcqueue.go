package rpcqueue

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/labstack/echo/v4"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/prometheus/client_golang/prometheus"
	zm "github.com/vmkteam/zenrpc-middleware"
	"github.com/vmkteam/zenrpc/v2"
)

const StreamName = "BROKERSRV"

const (
	maxAckWait    = 5 * time.Minute
	maxAckPending = 1000
)

type Message struct {
	Request json.RawMessage `json:"request"`
	Header  http.Header     `json:"header"`
}

type RPCQueue struct {
	subject string
	js      jetstream.JetStream
	srv     zenrpc.Server
	pf      Print
	ctx     context.Context
}

type Print func(ctx context.Context, msg string, args ...any)

var (
	statEvents = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "app",
		Subsystem: "rpcqueue",
		Name:      "events_total",
		Help:      "RPC queue events distributions.",
	}, []string{"type", "subject"})

	registerMetricsOnce sync.Once
)

// New initialize new brokersrv rpc queue.
func New(subject string, js jetstream.JetStream, srv zenrpc.Server, pf Print) RPCQueue {
	registerMetricsOnce.Do(func() {
		prometheus.MustRegister(statEvents)
	})

	return RPCQueue{
		subject: subject,
		js:      js,
		srv:     srv,
		pf:      pf,
	}
}

// Run subscribe to NATs Streaming subject and process events
func (q *RPCQueue) Run(ctx context.Context) error {
	c, err := q.js.CreateOrUpdateConsumer(ctx, StreamName, jetstream.ConsumerConfig{
		Name:          fmt.Sprintf("dur-%s", q.subject),
		FilterSubject: fmt.Sprintf("%s.%s", StreamName, q.subject),
		Durable:       fmt.Sprintf("dur-%s", q.subject),
		AckPolicy:     jetstream.AckExplicitPolicy,
		MaxAckPending: maxAckPending,
		AckWait:       maxAckWait,
	})
	if err != nil {
		return err
	}

	q.ctx = ctx // set base context from Run

	_, err = c.Consume(q.handleMessage)
	if err != nil {
		return err
	}

	return nil
}

// handleMessage send message to rpc server and acknowledge event.
func (q *RPCQueue) handleMessage(message jetstream.Msg) {
	var (
		m         Message
		zenrpcReq zenrpc.Request
	)

	err := json.Unmarshal(message.Data(), &m)
	if err != nil {
		statEvents.WithLabelValues("error", q.subject).Inc()
		q.pf(q.ctx, "failed to unmarshal message ", "err", err)
		return
	}

	err = json.Unmarshal(m.Request, &zenrpcReq)
	if err != nil {
		statEvents.WithLabelValues("error", q.subject).Inc()
		q.pf(q.ctx, "failed to unmarshal zenrpc request", "err", err)
		return
	}

	ctx := q.newContext(q.ctx, m.Header)
	_, err = q.srv.Do(ctx, m.Request)
	if err != nil {
		statEvents.WithLabelValues("error", q.subject).Inc()
		q.pf(ctx, "failed to send request to rpc server", "err", err)
		return
	}

	if err = message.Ack(); err != nil {
		statEvents.WithLabelValues("error", q.subject).Inc()
		q.pf(ctx, "failed to ack", "message", string(message.Data()), "err", err)
		return
	}

	statEvents.WithLabelValues("success", q.subject).Inc()
}

// newContext create new context with data from headers.
func (q *RPCQueue) newContext(ctx context.Context, h http.Header) context.Context {
	ctx = zm.NewIPContext(ctx, "127.0.0.1")
	ctx = zm.NewXRequestIDContext(ctx, h.Get(echo.HeaderXRequestID))
	ctx = zm.NewUserAgentContext(ctx, h.Get("User-Agent"))
	ctx = zm.NewVersionContext(ctx, h.Get("Version"))
	ctx = zm.NewPlatformContext(ctx, h.Get("Platform"))

	return ctx
}
