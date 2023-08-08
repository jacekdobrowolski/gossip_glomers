package main

import (
	"encoding/json"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"log"
	"sync"
)

type broadcastNodeServer struct {
	n             *maelstrom.Node
	messagesMutex sync.RWMutex
	messages      map[int]struct{}
	topologyMutex sync.RWMutex
	topology      []string
}

func (s *broadcastNodeServer) getMessages() []int {
	s.messagesMutex.RLock()
	messages := make([]int, len(s.messages))
	for message := range s.messages {
		messages = append(messages, message)
	}
	s.messagesMutex.RUnlock()
	return messages
}

func (s *broadcastNodeServer) getMessagesCount() int {
	s.messagesMutex.RLock()
	defer s.messagesMutex.RUnlock()
	return len(s.messages)
}

func main() {

	s := broadcastNodeServer{
		n:        maelstrom.NewNode(),
		messages: make(map[int]struct{}),
	}

	s.n.Handle("broadcast", func(msg maelstrom.Message) error {
		type broadcastMsgType struct {
			Message int `json:"message"`
		}
		var body broadcastMsgType
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		if err := s.gossipToNeighbours(body.Message); err != nil {
			return err
		}

		return s.n.Reply(msg, map[string]any{
			"type": "broadcast_ok",
		})
	})

	s.n.Handle("gossip", func(msg maelstrom.Message) error {
		type broadcastOkMsgType struct {
			Type         string `json:"type"`
			Message      int    `json:"message"`
			MessageCount int    `json:"messages_count"`
		}
		var broadcastOkMsg broadcastOkMsgType
		if err := json.Unmarshal(msg.Body, &broadcastOkMsg); err != nil {
			return err
		}

		if err := s.gossipToNeighbours(broadcastOkMsg.Message); err != nil {
			return err
		}

		if broadcastOkMsg.MessageCount < s.getMessagesCount() {
			return s.n.Send(msg.Src, map[string]any{
				"type": "read",
			})
		}

		return nil
	})

	s.n.Handle("read", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		return s.n.Reply(msg, map[string]any{
			"type":     "read_ok",
			"messages": s.getMessages(),
		})
	})

	s.n.Handle("read_ok", func(msg maelstrom.Message) error {
		type readOkMsgType struct {
			Type     string `json:"type"`
			Messages []int  `json:"messages"`
		}
		var readOkMsg readOkMsgType
		if err := json.Unmarshal(msg.Body, &readOkMsg); err != nil {
			return err
		}
		s.messagesMutex.Lock()
		for message := range readOkMsg.Messages {
			s.messages[message] = struct{}{}
		}
		s.messagesMutex.Unlock()
		return nil
	})

	s.n.Handle("topology", func(msg maelstrom.Message) error {
		type topologyMsgType struct {
			Topology map[string][]string `json:"topology"`
		}
		var topologyMsg topologyMsgType
		if err := json.Unmarshal(msg.Body, &topologyMsg); err != nil {
			return err
		}

		s.topologyMutex.Lock()
		s.topology = topologyMsg.Topology[msg.Dest]
		s.topologyMutex.Unlock()

		return s.n.Reply(msg, map[string]any{
			"type": "topology_ok",
		})
	})

	if err := s.n.Run(); err != nil {
		log.Fatal(err)
	}
}

func (s *broadcastNodeServer) gossipToNeighbours(message int) error {
	s.messagesMutex.Lock()
	// node already has this message
	if _, exists := s.messages[message]; exists {
		s.messagesMutex.Unlock()
		return nil
	}
	s.messages[message] = struct{}{}
	s.messagesMutex.Unlock()

	respBody := map[string]any{
		"type":          "gossip",
		"message":       message,
		"message_count": s.getMessagesCount(),
	}

	s.topologyMutex.RLock()
	for _, neighbour := range s.topology {
		neighbour := neighbour
		go func() {
			if err := s.n.Send(neighbour, respBody); err != nil {
				panic(err)
			}
		}()
	}
	s.topologyMutex.RUnlock()
	return nil
}
