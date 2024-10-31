.PHONY: clean serve maelstrom-echo maelstrom-unique-ids

BREW_PREFIX := $(shell brew --prefix)

maelstrom-fault-tolerant-broadcast: gossip-glomers maelstrom/maelstrom
	./maelstrom/maelstrom test -w broadcast --bin ./gossip-glomers --node-count 5 --time-limit 20 --rate 10 --nemesis partition

maelstrom-multi-node-broadcast: gossip-glomers maelstrom/maelstrom
	./maelstrom/maelstrom test -w broadcast --bin ./gossip-glomers --node-count 5 --time-limit 20 --rate 10

maelstrom-single-node-broadcast: gossip-glomers maelstrom/maelstrom
	./maelstrom/maelstrom test -w broadcast --bin ./gossip-glomers --node-count 1 --time-limit 20 --rate 10

maelstrom-unique-ids: gossip-glomers maelstrom/maelstrom
	./maelstrom/maelstrom test -w unique-ids --bin ./gossip-glomers --time-limit 30 --rate 1000 --node-count 3 --availability total --nemesis partition

maelstrom-echo: gossip-glomers maelstrom/maelstrom
	./maelstrom/maelstrom test -w echo --bin ./gossip-glomers --node-count 1 --time-limit 10

gossip-glomers: *.go
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