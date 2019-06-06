package chat

import (
	"sync"

	"github.com/hashicorp/go-multierror"
)

// Publisher is implemented by objects that can send a message to a user.
type Publisher interface {
	Publish(userID, message string) error
}

// Room models a chat room.
type Room struct {
	pub Publisher

	mu    sync.RWMutex
	users []string
}

// NewRoom creates a new chat root instance that ued pub to broadcast messages.
func NewRoom(pub Publisher) *Room {
	return &Room{pub: pub}
}

// AddUser appends user to the room user list.
func (r *Room) AddUser(user string) {
	r.mu.Lock()
	r.users = append(r.users, user)
	r.mu.Unlock()
}

// Broadcast message to all users currently in the room.
func (r *Room) Broadcast(message string) error {
	r.mu.RLock()
	var err error
	for _, user := range r.users {
		if pErr := r.pub.Publish(user, message); pErr != nil {
			err = multierror.Append(err, pErr)
		}
	}

	r.mu.RUnlock()
	return err
}
