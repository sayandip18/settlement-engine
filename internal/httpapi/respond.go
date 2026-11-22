// Package httpapi holds HTTP plumbing shared by all handlers.
package httpapi

import (
	"encoding/json"
	"log/slog"
	"net/http"
)

type errorBody struct {
	Error struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

func WriteJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		slog.Error("write response", "err", err)
	}
}

func WriteError(w http.ResponseWriter, status int, code, message string) {
	var b errorBody
	b.Error.Code = code
	b.Error.Message = message
	WriteJSON(w, status, b)
}
