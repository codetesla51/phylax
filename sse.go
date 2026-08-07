package phylax

import (
	"encoding/json"
	"net/http"
)

type Server struct {
	broadcaster *Broadcaster
}

func NewServer() *Server {
	return &Server{
		broadcaster: NewBroadcaster(),
	}
}
func (s *Server) NewSSEHandler(w http.ResponseWriter, r *http.Request) {
	// Set the headers for SSE
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	fush, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "Streaming unsupported!", http.StatusInternalServerError)
		return
	}

	// Create a new subscriber channel
	subscriberID := r.RemoteAddr // or any unique identifier for the subscriber
	ch := s.broadcaster.Subscribe(subscriberID, 10)
	defer s.broadcaster.Unsubscribe(subscriberID)

	for {
		select {
		case change := <-ch:
			// Write the change to the response
			changeJSON, err := json.Marshal(change)
			if err != nil {
				return
			}
			_, err = w.Write([]byte("data: " + string(changeJSON) + "\n\n"))
			if err != nil {
				return
			}
			fush.Flush()
		case <-r.Context().Done():
			return
		}

	}
}
