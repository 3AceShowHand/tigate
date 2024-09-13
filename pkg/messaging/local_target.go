package messaging

import (
	"context"
	. "github.com/flowbehappy/tigate/pkg/apperror"
	"github.com/flowbehappy/tigate/pkg/metrics"
	"github.com/flowbehappy/tigate/pkg/node"
	"github.com/prometheus/client_golang/prometheus"
	"sync/atomic"
)

// localMessageTarget implements the SendMessageChannel interface.
// It is used to send messages to the local server.
// It simply pushes the messages to the messageCenter's channel directly.
type localMessageTarget struct {
	localId  node.ID
	epoch    uint64
	sequence atomic.Uint64

	// The gather channel from the message center.
	// We only need to push and pull the messages from those channel.
	recvEventCh chan *TargetMessage
	recvCmdCh   chan *TargetMessage

	sendEventCounter   prometheus.Counter
	dropMessageCounter prometheus.Counter
	sendCmdCounter     prometheus.Counter
}

func (s *localMessageTarget) Epoch() uint64 {
	return s.epoch
}

func (s *localMessageTarget) sendEvent(ctx context.Context, msg *TargetMessage) error {
	err := s.sendMsgToChan(ctx, s.recvEventCh, msg)
	if err != nil {
		s.recordCongestedMessageError(msgTypeEvent)
	} else {
		s.sendEventCounter.Inc()
	}
	return err
}

func (s *localMessageTarget) sendCommand(ctx context.Context, msg *TargetMessage) error {
	err := s.sendMsgToChan(ctx, s.recvCmdCh, msg)
	if err != nil {
		s.recordCongestedMessageError(msgTypeCommand)
	} else {
		s.sendCmdCounter.Inc()
	}
	return err
}

func newLocalMessageTarget(id node.ID,
	gatherRecvEventChan chan *TargetMessage,
	gatherRecvCmdChan chan *TargetMessage,
) *localMessageTarget {
	return &localMessageTarget{
		localId:            id,
		recvEventCh:        gatherRecvEventChan,
		recvCmdCh:          gatherRecvCmdChan,
		sendEventCounter:   metrics.MessagingSendMsgCounter.WithLabelValues("local", "event"),
		dropMessageCounter: metrics.MessagingDropMsgCounter.WithLabelValues("local", "message"),
		sendCmdCounter:     metrics.MessagingSendMsgCounter.WithLabelValues("local", "command"),
	}
}

func (s *localMessageTarget) recordCongestedMessageError(typeE string) {
	metrics.MessagingErrorCounter.WithLabelValues("local", typeE, "message_congested").Inc()
}

func (s *localMessageTarget) sendMsgToChan(ctx context.Context, ch chan *TargetMessage, msg ...*TargetMessage) error {
	for i, m := range msg {
		m.To = s.localId
		m.From = s.localId
		m.Epoch = s.epoch
		m.Sequence = s.sequence.Add(1)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case ch <- m:
		default:
			remains := len(msg) - i
			s.dropMessageCounter.Add(float64(remains))
			return AppError{Type: ErrorTypeMessageCongested, Reason: "Send message is congested"}
		}
	}
	return nil
}

type sendStreamWrapper struct {
	stream grpcSender
	ready  atomic.Bool
}
