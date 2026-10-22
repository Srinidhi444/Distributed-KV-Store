package transport

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
)

const maxPingRequestSize = 1024

func NewPingHandler(nodeID string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", http.MethodPost)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		r.Body = http.MaxBytesReader(w, r.Body, maxPingRequestSize)
		defer r.Body.Close()

		decoder := json.NewDecoder(r.Body)
		decoder.DisallowUnknownFields()

		var req PingRequest

		if err := decoder.Decode(&req); err != nil {
			http.Error(w, "invalid ping request", http.StatusBadRequest)
			return
		}

		if strings.TrimSpace(req.From) == "" {
			http.Error(
				w,
				"ping sender cannot be empty",
				http.StatusBadRequest,
			)
			return
		}

		// The second decode must reach the end of the body.
		// Any additional JSON value or malformed trailing data
		// makes the request invalid.
		var extra json.RawMessage

		if err := decoder.Decode(&extra); err != io.EOF {
			http.Error(
				w,
				"request must contain exactly one JSON object",
				http.StatusBadRequest,
			)
			return
		}

		resp := PingResponse{
			NodeID:  nodeID,
			Message: "pong",
		}

		w.Header().Set("Content-Type", "application/json")

		// Encoding a small fixed response is not expected to fail
		// during serialization, but writing to a disconnected client
		// can still fail.
		_ = json.NewEncoder(w).Encode(resp)
	})
}
