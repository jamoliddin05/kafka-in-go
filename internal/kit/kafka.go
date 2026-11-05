package kit

import (
	"context"
	"errors"
	"fmt"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/plugin/kslog"
)

// NewClient returns a franz-go client pointed at the course cluster.
// Options passed in are applied after the defaults, so they win.
func (a *App) NewClient(opts ...kgo.Opt) (*kgo.Client, error) {
	return kgo.NewClient(a.ClientOpts(opts...)...)
}

// ClientOpts is the course defaults followed by opts, for constructors that
// build their own client, such as kgo.NewGroupTransactSession.
func (a *App) ClientOpts(opts ...kgo.Opt) []kgo.Opt {
	base := []kgo.Opt{
		kgo.SeedBrokers(a.Brokers...),
		// Shows up in `kafka-consumer-groups --members` and broker request logs:
		// in prod this is how you find which deployment owns a misbehaving member.
		kgo.ClientID(fmt.Sprintf("%s.%s", a.Task, a.Role)),
		kgo.WithLogger(kslog.New(a.kafkaLog)),
	}
	return append(base, opts...)
}

// Topic describes a topic the task needs.
type Topic struct {
	Name       string
	Partitions int32
	// RF 0 means the broker default, which is 3 in this cluster.
	RF      int16
	Configs map[string]string
}

// EnsureTopic creates the topic if it does not exist. An existing topic is
// left untouched even if it differs: in prod, changing partitions or configs
// of a live topic is a deliberate operation, never a side effect of a deploy.
func (a *App) EnsureTopic(ctx context.Context, cl *kgo.Client, t Topic) error {
	rf := t.RF
	if rf == 0 {
		rf = -1
	}
	configs := make(map[string]*string, len(t.Configs))
	for k, v := range t.Configs {
		configs[k] = &v
	}

	_, err := kadm.NewClient(cl).CreateTopic(ctx, t.Partitions, rf, configs, t.Name)
	switch {
	case errors.Is(err, kerr.TopicAlreadyExists):
		a.Log.Debug("topic already exists", "topic", t.Name)
		return nil
	case err != nil:
		return fmt.Errorf("create topic %s: %w", t.Name, err)
	}
	a.Log.Info("topic created", "topic", t.Name, "partitions", t.Partitions, "configs", t.Configs)
	return nil
}
