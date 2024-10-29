package main

import (
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"log"
)

func main() {
	n := maelstrom.NewNode()

	n.Handle("echo", func(msg maelstrom.Message) error {
		return echo(n, msg)
	})

	n.Handle("generate", func(msg maelstrom.Message) error {
		return generate(n, msg)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
