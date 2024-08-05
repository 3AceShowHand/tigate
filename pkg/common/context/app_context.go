package context

import (
	"github.com/flowbehappy/tigate/pkg/messaging"
	"sync"
)

var (
	instance *AppContext
	once     sync.Once
)

const (
	MessageCenter      = "MessageCenter"
	EventCollector     = "EventCollector"
	HeartbeatCollector = "HeartbeatCollector"
	EventStore         = "EventStore"
	EventService       = "EventService"
)

// Put all the global instances here.
type AppContext struct {
	// TODO
	serviceMap sync.Map

	mc messaging.MessageCenter
}

func GetGlobalContext() *AppContext {
	once.Do(func() {
		instance = &AppContext{
			// Initialize fields here
		}
	})
	return instance
}

func SetService[T any](name string, t T) { GetGlobalContext().serviceMap.Store(name, t) }
func GetService[T any](name string) T {
	v, _ := GetGlobalContext().serviceMap.Load(name)
	return v.(T)
}

func GetMessageCenter() messaging.MessageCenter {
	return GetGlobalContext().mc
}
