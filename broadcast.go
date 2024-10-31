package main

import (
	"context"
	"encoding/json"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"sync"
	"time"
)

type BroadcastMessageBody struct {
	Message int
}

type BroadcastMessage struct {
	maelstrom.Message
	Body BroadcastMessageBody
}

type Topology = map[string][]string

type TopologyMessageBody struct {
	Topology Topology
}

// MessageStore keeps track of which messages we've received, replicates them to other nodes, and supports concurrent
// reads and writes.
type MessageStore struct {
	mutex sync.Mutex
	// We trade off memory usage for throughput by storing messages in both a set and a slice, giving us constant time
	// reads and writes.
	hashMap          map[int]bool
	slice            []int
	replicationQueue chan<- BroadcastMessage
}

func (s *MessageStore) Initialize(n *maelstrom.Node, topology Topology) {
	s.replicationQueue = startReplicationWorkers(n, topology[n.ID()])
}

func (s *MessageStore) Write(message BroadcastMessage) bool {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	_, ok := s.hashMap[message.Body.Message]

	if !ok {
		s.hashMap[message.Body.Message] = true
		s.slice = append(s.slice, message.Body.Message)
		s.replicationQueue <- message
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

	n.Handle("topology", func(msg maelstrom.Message) error {
		var body TopologyMessageBody
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		store.Initialize(n, body.Topology)

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

		store.Write(message)

		return n.Reply(msg, map[string]any{
			"type": "broadcast_ok",
		})
	})

	handle(n, "read", func(body map[string]any) (reply any, err error) {
		reply = map[string]any{
			"type":     "read_ok",
			"messages": store.Read(),
		}

		return
	})

}

func startReplicationWorkers(n *maelstrom.Node, neighbours []string) chan<- BroadcastMessage {
	input := make(chan BroadcastMessage, 128)

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
				"type":    "broadcast",
				"message": message.Body.Message,
			}

			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()

			reply, err := n.SyncRPC(ctx, dest, request)

			if err != nil || reply.Type() != "broadcast_ok" {
				queue <- message
			}
		}
	}()

	for message := range input {
		queue <- message
	}
}
