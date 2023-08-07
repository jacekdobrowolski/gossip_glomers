package main

import (
	"encoding/json"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"log"
	"sync"
)

type broadcastNodeDataStore struct {
	messagesMutex sync.RWMutex
	messages      []int
	topologyMutex sync.RWMutex
	topology      map[string][]string
}

func main() {
	n := maelstrom.NewNode()
	bnds := broadcastNodeDataStore{}

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		type broadcastMsgType struct {
			Message int `json:"message"`
		}
		var body broadcastMsgType
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		bnds.messagesMutex.Lock()
		bnds.messages = append(bnds.messages, body.Message)
		bnds.messagesMutex.Unlock()

		return n.Reply(msg, map[string]any{
			"type": "broadcast_ok",
		})
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		bnds.messagesMutex.RLock()
		messages := make([]int, len(bnds.messages))
		copy(messages, bnds.messages)
		bnds.messagesMutex.RUnlock()

		return n.Reply(msg, map[string]any{
			"type":     "read_ok",
			"messages": messages,
		})
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
		type topologyMsgType struct {
			Topology map[string][]string `json:"topology"`
		}
		var topologyMsg topologyMsgType
		if err := json.Unmarshal(msg.Body, &topologyMsg); err != nil {
			return err
		}

		bnds.topologyMutex.Lock()
		bnds.topology = topologyMsg.Topology
		bnds.topologyMutex.Unlock()

		return n.Reply(msg, map[string]any{
			"type": "topology_ok",
		})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
