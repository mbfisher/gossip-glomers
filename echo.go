package main

import (
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func echo(n *maelstrom.Node) {
	handle(n, "echo", func(body map[string]any) (any, error) {
		body["type"] = "echo_ok"
		return body, nil
	})
}
