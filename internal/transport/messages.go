package transport

type PingRequest struct {
	From string `json:"from"`
}

type PingResponse struct {
	NodeID  string `json:"node_id"`
	Message string `json:"message"`
}
