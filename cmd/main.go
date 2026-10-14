package main

import (
	"encoding/json"
	"log"
	"net/http"
	"time"

	"ingest/internal/lib"
	"ingest/internal/storage"
)

func main() {
	// Storage initialization
	storage, err := storage.New("./uploads")
	if err != nil {
		log.Fatal(err)
	}
	// Pass storage to the component that needs it.
	_ = storage

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", health)
	mux.HandleFunc("GET /hello", hello)
	mux.HandleFunc("POST /echo", echo)

	server := &http.Server{
		Addr:              lib.Getenv("PORT", ":8080"),
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	log.Printf("listening on %s", server.Addr)
	log.Fatal(server.ListenAndServe())

}

func health(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
}

func hello(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"message": "hello"})
}

func echo(w http.ResponseWriter, r *http.Request) {
	var request struct {
		Text string `json:"text"`
	}

	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	if request.Text == "" {
		writeError(w, http.StatusUnprocessableEntity, "text is required")
		return
	}

	writeJSON(w, http.StatusOK, request)
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}
