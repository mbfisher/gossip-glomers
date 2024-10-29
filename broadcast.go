package main

import (
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

var messages []int

func broadcast(n *maelstrom.Node) {
	handle(n, "broadcast", func(body map[string]any) (reply any, err error) {
		message := int(body["message"].(float64))
		messages = append(messages, message)

		reply = map[string]any{
			"type": "broadcast_ok",
		}

		return
	})

	handle(n, "read", func(body map[string]any) (reply any, err error) {
		reply = map[string]any{
			"type":     "read_ok",
			"messages": messages,
		}

		return
	})

	handle(n, "topology", func(body map[string]any) (reply any, err error) {
		reply = map[string]any{
			"type": "topology_ok",
		}

		return
	})
}
