package main

import (
	"encoding/json"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"log"
)

func unmarshal(msg maelstrom.Message) map[string]any {
	var body map[string]any

	if err := json.Unmarshal(msg.Body, &body); err != nil {
		log.Fatal(err)
	}

	return body
}

func handle(n *maelstrom.Node, rpc string, handler func(body map[string]any) (reply any, err error)) {
	n.Handle(rpc, func(msg maelstrom.Message) error {
		body := unmarshal(msg)
		reply, err := handler(body)

		if err != nil {
			return err
		}

		return n.Reply(msg, reply)
	})
}

func main() {
	n := maelstrom.NewNode()

	echo(n)
	generate(n)
	broadcast(n)

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
