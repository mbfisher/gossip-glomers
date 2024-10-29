package main

import (
	"github.com/google/uuid"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func generate(n *maelstrom.Node) {
	handle(n, "generate", func(body map[string]any) (any, error) {
		body["type"] = "generate_ok"
		body["id"] = uuid.New().String()
		return body, nil
	})
}
