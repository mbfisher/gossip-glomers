package main

import (
	"encoding/json"
	"github.com/google/uuid"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func generate(n *maelstrom.Node, msg maelstrom.Message) error {
	var body map[string]any

	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	body["type"] = "generate_ok"
	body["id"] = uuid.New().String()

	return n.Reply(msg, body)
}
