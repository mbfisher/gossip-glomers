package main

import (
	"encoding/json"
	"errors"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"strings"
	"sync"
)

type broadcastMessageBody struct {
	Message  int
	Received map[string]bool
}

type topologyMessageBody struct {
	Topology map[string][]string
}

// When each node receives a broadcast message it "commits" it to a store of messages it's seen.
// It then forwards the broadcast message on to its direct neighbours.
// Because the topology graph contains cycles, each node can see a broadcast message more than once.
// This acts as redundancy: there are multiple paths between sending and receiving nodes, which means if one of the
// traversals fails the message can still be received via a different one.
// https://mermaid.live/edit#pako:eNpVkL1uhDAQhF8FTc2dMMa54CLVtamSKnKzwuZAAvvk2EouiHePAyiSt9pvf2ZXs6Bz2kCin9xXN5APxftV2SKFrYrT6aWwPEd2INuxybHOsTqwznd53uW5VJMPH8hRYjZ-plGnd5e_pkIYzGwUZEq16SlOQUHZNY1SDO7tYTvI4KMpEe-agrmOdPM0Q_Y0fabqnSzkgm_IVpw5axvGq7YWonou8YAUZyaaWjDOni6XNpXFWuLHuSTAtuWPLd8veBdvw7-y0WNw_nU3d_N4_QUTTWR0
func broadcast(n *maelstrom.Node) {
	var topology map[string][]string

	var messages []int
	committed := make(map[int]bool)
	commitMutex := sync.Mutex{}

	write := func(message int) bool {
		commitMutex.Lock()
		defer commitMutex.Unlock()

		if _, ok := committed[message]; ok {
			return false
		}

		messages = append(messages, message)
		committed[message] = true

		return true
	}

	n.Handle("topology", func(msg maelstrom.Message) error {
		var body topologyMessageBody
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		topology = body.Topology

		return n.Reply(msg, map[string]any{
			"type": "topology_ok",
		})
	})

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body broadcastMessageBody
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		message := body.Message

		if written := write(message); !written {
			return nil
		}

		if len(body.Received) == 0 {
			body.Received = make(map[string]bool)
		}

		body.Received[n.ID()] = true

		neighbours, ok := topology[n.ID()]

		if !ok {
			return errors.New("node not found in topology")
		}

		for _, neighbour := range neighbours {
			if _, ok := body.Received[neighbour]; ok {
				continue
			}

			err := n.Send(neighbour, map[string]any{
				"type":     "broadcast",
				"message":  message,
				"received": body.Received,
			})

			if err != nil {
				return err
			}
		}

		if strings.HasPrefix(msg.Src, "n") {
			return nil
		}

		return n.Reply(msg, map[string]any{
			"type": "broadcast_ok",
		})
	})

	handle(n, "read", func(body map[string]any) (reply any, err error) {
		reply = map[string]any{
			"type":     "read_ok",
			"messages": messages,
		}

		return
	})
}
