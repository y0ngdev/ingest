package main

import (
	"log"
	"net/http"
	"time"

	"ingest/internal/api"
	"ingest/internal/dedup"
	"ingest/internal/lib"
	"ingest/internal/services"
	"ingest/internal/storage"
)

func main() {
	// --- Storage ---
	// To use any S3-compatible backend instead of local disk, swap these lines:
	//
	//   store, err := storage.NewS3(storage.S3Config{
	//       Bucket:    lib.Getenv("S3_BUCKET", "ingest"),
	//       Endpoint:  lib.Getenv("S3_ENDPOINT", ""),  // empty = AWS; set for MinIO/R2/B2/etc.
	//       PathStyle: lib.Getenv("S3_PATH_STYLE", "") != "",
	//   })
	store, err := storage.New("./uploads")
	if err != nil {
		log.Fatalf("storage: %v", err)
	}

	// --- Session store + dedup index ---
	sessions := services.NewStore()
	dedupIdx := dedup.New()

	// --- Handlers ---
	uploads := api.New(store, sessions, dedupIdx)

	// --- Routes ---
	mux := http.NewServeMux()

	mux.HandleFunc("GET /healthz", health)

	// Single-shot upload (Phase 1)
	mux.HandleFunc("PUT /uploads/{id}", uploads.WriteUpload)
	mux.HandleFunc("GET /uploads/{id}", uploads.GetUpload)

	// Resumable chunked upload (Phase 2)
	mux.HandleFunc("POST /uploads", uploads.CreateUpload)
	mux.HandleFunc("PATCH /uploads/{id}", uploads.AppendChunk)
	mux.HandleFunc("HEAD /uploads/{id}", uploads.HeadUpload)

	// --- Server ---
	server := &http.Server{
		Addr:              lib.Getenv("PORT", ":8080"),
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		// WriteTimeout intentionally 0: large uploads stream through here.
		WriteTimeout: 0,
		IdleTimeout:  60 * time.Second,
	}

	log.Printf("listening on %s", server.Addr)
	log.Fatal(server.ListenAndServe())
}

func health(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
}
