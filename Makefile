.PHONY: clean serve

BREW_PREFIX := $(shell brew --prefix)

maelstrom-echo: gossip-glomers maelstrom/maelstrom
	./maelstrom/maelstrom test -w echo --bin ./gossip-glomers --node-count 1 --time-limit 10

gossip-glomers: main.go
	go build .

maelstrom/maelstrom: $(BREW_PREFIX)/Cellar/graphviz $(BREW_PREFIX)/Cellar/gnuplot
	wget https://github.com/jepsen-io/maelstrom/releases/download/v0.2.3/maelstrom.tar.bz2 -O - | tar -xjf -
	touch maelstrom/maelstrom

$(BREW_PREFIX)/Cellar/graphviz $(BREW_PREFIX)/Cellar/gnuplot:
	brew install graphviz gnuplot

serve:
	maelstrom/maelstrom serve

clean:
	rm -rf maelstrom/ gossip-glomers