package handlers

type EventHandlers struct {
	EventHandler *EventHandler
}

func NewEventHandlers(eventHandler *EventHandler) *EventHandlers {
	return &EventHandlers{
		EventHandler: eventHandler,
	}
}
