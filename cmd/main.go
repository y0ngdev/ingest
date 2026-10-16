package main

import (
	"log"
	"net/http"
	"time"

	"ingest/internal/api"
	"ingest/internal/lib"
	"ingest/internal/storage"
)

func main() {
	// --- Storage ---
	store, err := storage.New("./uploads")
	if err != nil {
		log.Fatalf("storage: %v", err)
	}

	// --- Handlers ---
	uploads := api.New(store)

	// --- Routes ---
	mux := http.NewServeMux()

	// Existing utility routes
	mux.HandleFunc("GET /healthz", health)

	// Upload session routes
	mux.HandleFunc("POST /uploads", uploads.CreateUpload)
	mux.HandleFunc("PUT /uploads/{id}", uploads.WriteUpload)
	mux.HandleFunc("GET /uploads/{id}", uploads.GetUpload)

	// --- Server ---
	server := &http.Server{
		Addr:              lib.Getenv("PORT", ":8080"),
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		// WriteTimeout is intentionally long: large uploads stream through here.
		WriteTimeout: 0,
		IdleTimeout:  60 * time.Second,
	}

	log.Printf("listening on %s", server.Addr)
	log.Fatal(server.ListenAndServe())
}

func health(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
}
