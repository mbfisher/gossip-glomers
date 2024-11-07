package main

import (
	"context"
	"encoding/json"
	"fmt"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"log"
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type BroadcastMessageBody struct {
	Message    int
	MsgId      int   `json:"msg_id,omitempty"`
	ReceivedAt int64 `json:"received_at,omitempty"`
}

type BroadcastMessage struct {
	maelstrom.Message
	Body BroadcastMessageBody
}

type Topology = map[string][]string

type TopologyMessageBody struct {
	Topology Topology
}

var (
	replicationQueueSize = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "replication_queue_size",
		},
		[]string{"src", "dest"},
	)
	replicationAttempts = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "replication_attempts_total",
		},
		[]string{"src", "dest"},
	)
	replicationErrors = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "replication_errors_total",
		},
		[]string{"src", "dest"},
	)
	replicatedMessages = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "replicated_messages_total",
		},
		[]string{"src", "dest"},
	)
	messagesReceived = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "messages_received_total",
		},
		[]string{"dest", "type", "src"},
	)
	replicationLag = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name: "replication_lag_millis",
			Buckets: []float64{
				50,
				100,
				250,
				500,
				1000,
			},
		},
		[]string{"src", "dest"},
	)
	storeSize = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "store_size",
		},
		[]string{"node"},
	)
)

// MessageStore keeps track of which messages we've received, replicates them to other nodes, and supports concurrent
// reads and writes.
type MessageStore struct {
	mutex sync.Mutex
	node  *maelstrom.Node
	// We trade off memory usage for throughput by storing messages in both a set and a slice, giving us constant time
	// reads and writes.
	hashMap          map[int]bool
	slice            []int
	replicationQueue chan<- BroadcastMessage
}

func (s *MessageStore) Initialize(n *maelstrom.Node, topology Topology) {
	s.node = n
	s.replicationQueue = startReplicationWorkers(n, topology[n.ID()])
}

func (s *MessageStore) Write(message BroadcastMessage) bool {
	s.mutex.Lock()

	_, ok := s.hashMap[message.Body.Message]

	if !ok {
		s.hashMap[message.Body.Message] = true
		s.slice = append(s.slice, message.Body.Message)
		storeSize.WithLabelValues(s.node.ID()).Inc()
	}

	s.mutex.Unlock()

	// Inter-server messages don't need to be replicated
	if strings.HasPrefix(message.Src, "c") {
		s.replicationQueue <- message
	}

	if message.Body.ReceivedAt > 0 {
		replicationLag.WithLabelValues(message.Src, s.node.ID()).Observe(float64(time.Now().UTC().UnixMilli() - message.Body.ReceivedAt))
	}

	return !ok
}

func (s *MessageStore) Read() []int {
	return s.slice
}

func newMessageStore() *MessageStore {
	return &MessageStore{
		mutex:   sync.Mutex{},
		hashMap: make(map[int]bool),
		slice:   make([]int, 0, 128),
	}
}

// When each node receives a broadcast message it "commits" it to a store of messages it's seen.
// It then forwards the broadcast message on to its direct neighbours.
// Because the topology graph contains cycles, each node can see a broadcast message more than once.
// This could be optimised to track which nodes have seen a message to avoid duplicate replication attempts.
// https://mermaid.live/edit#pako:eNpVkL1uhDAQhF8FTc2dMMa54CLVtamSKnKzwuZAAvvk2EouiHePAyiSt9pvf2ZXs6Bz2kCin9xXN5APxftV2SKFrYrT6aWwPEd2INuxybHOsTqwznd53uW5VJMPH8hRYjZ-plGnd5e_pkIYzGwUZEq16SlOQUHZNY1SDO7tYTvI4KMpEe-agrmOdPM0Q_Y0fabqnSzkgm_IVpw5axvGq7YWonou8YAUZyaaWjDOni6XNpXFWuLHuSTAtuWPLd8veBdvw7-y0WNw_nU3d_N4_QUTTWR0
func broadcast(n *maelstrom.Node) {
	store := newMessageStore()

	reg := prometheus.NewRegistry()

	reg.MustRegister(
		replicationQueueSize,
		replicationAttempts,
		replicatedMessages,
		replicationErrors,
		messagesReceived,
		replicationLag,
		storeSize,
		collectors.NewGoCollector(
			collectors.WithGoCollectorRuntimeMetrics(
				collectors.GoRuntimeMetricsRule{Matcher: regexp.MustCompile("/sched/latencies:seconds")},
			),
		),
	)

	http.Handle("/metrics", promhttp.HandlerFor(
		reg,
		promhttp.HandlerOpts{
			// Opt into OpenMetrics to support exemplars.
			EnableOpenMetrics: true,
		}),
	)

	n.Handle("topology", func(msg maelstrom.Message) error {
		var body TopologyMessageBody
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		// Default topology :grid
		// 00 - 01 - 02 - 03 - 04
		// |    |    |    |    |
		// 05 - 06 - 07 - 08 - 09
		// |    |    |    |    |
		// 10 - 11 - 12 - 13 - 14
		// |    |    |    |    |
		// 15 - 16 - 17 - 18 - 19
		// |    |    |    |    |
		// 20 - 21 - 22 - 23 - 24

		nodeCount := len(body.Topology)
		everyNodeIsMyNeighbourTopology := make(Topology)

		for src := 0; src < nodeCount; src++ {
			for dest := 0; dest < nodeCount; dest++ {
				if src == dest {
					continue
				}

				srcId := fmt.Sprintf("n%d", src)
				destId := fmt.Sprintf("n%d", dest)

				if _, ok := everyNodeIsMyNeighbourTopology[srcId]; !ok {
					everyNodeIsMyNeighbourTopology[srcId] = make([]string, 0, nodeCount-1)
				}

				everyNodeIsMyNeighbourTopology[srcId] = append(everyNodeIsMyNeighbourTopology[srcId], destId)
			}
		}

		store.Initialize(n, everyNodeIsMyNeighbourTopology)

		id, err := strconv.Atoi(strings.TrimPrefix(n.ID(), "n"))
		if err != nil {
			panic(err)
		}

		metricsPort := fmt.Sprintf("localhost:211%d", id)

		go func() {
			log.Printf("Prometheus listening on %s\n", metricsPort)
			log.Fatal(http.ListenAndServe(metricsPort, nil))
		}()

		return n.Reply(msg, map[string]any{
			"type": "topology_ok",
		})
	})

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		message := BroadcastMessage{
			Message: msg,
		}

		if err := json.Unmarshal(msg.Body, &message.Body); err != nil {
			return err
		}

		log.Printf("%s received %d from %s", n.ID(), message.Body.Message, msg.Src)
		message.Body.ReceivedAt = time.Now().UTC().UnixMilli()
		store.Write(message)

		messagesReceived.WithLabelValues(n.ID(), "broadcast", msg.Src).Inc()

		return n.Reply(msg, map[string]any{
			"type": "broadcast_ok",
		})
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		messagesReceived.WithLabelValues(n.ID(), "read", msg.Src).Inc()

		return n.Reply(msg, map[string]any{
			"type":     "read_ok",
			"messages": store.Read(),
		})
	})
}

func startReplicationWorkers(n *maelstrom.Node, neighbours []string) chan<- BroadcastMessage {
	input := make(chan BroadcastMessage, 128)
	sort.Strings(neighbours)

	log.Printf("%s replicating to %v\n", n.ID(), neighbours)

	neighbourChans := make(map[string]chan BroadcastMessage)
	for _, neighbour := range neighbours {
		neighbourChans[neighbour] = startReplicationWorker(n, neighbour)
	}

	go fanoutWorker(input, neighbourChans)

	return input
}

func fanoutWorker(input <-chan BroadcastMessage, neighbourChans map[string]chan BroadcastMessage) {
	for message := range input {
		for dest, ch := range neighbourChans {
			if dest != message.Src {
				ch <- message
			}
		}
	}
}

func startReplicationWorker(n *maelstrom.Node, dest string) chan BroadcastMessage {
	input := make(chan BroadcastMessage, 128)

	go replicationWorker(n, dest, input)

	return input
}

func replicationWorker(n *maelstrom.Node, dest string, input <-chan BroadcastMessage) {
	// We implement retries by using this queue as a ring buffer.
	// Whenever a message is not acked by a neighbour we re-enqueue it.
	// When a neighbour is down, the queue grows as more and more messages haven't been acked. This acts kind of like
	// exponential back-off - the longer the queue, the greater the interval between retrying any one message.
	queue := make(chan BroadcastMessage, 1024)

	go func() {
		for message := range queue {
			request := map[string]any{
				"type":        "broadcast",
				"message":     message.Body.Message,
				"received_at": message.Body.ReceivedAt,
			}

			log.Printf("%s replicating %d to %s", n.ID(), message.Body.Message, dest)
			replicationAttempts.WithLabelValues(n.ID(), dest).Add(1)

			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()

			reply, err := n.SyncRPC(ctx, dest, request)

			if err != nil || reply.Type() != "broadcast_ok" {
				replicationErrors.WithLabelValues(n.ID(), dest).Inc()
				queue <- message
			} else {
				replicationQueueSize.WithLabelValues(n.ID(), dest).Sub(1)
				replicatedMessages.WithLabelValues(n.ID(), dest).Inc()
			}
		}
	}()

	for message := range input {
		queue <- message
		replicationQueueSize.WithLabelValues(n.ID(), dest).Add(1)
	}
}
