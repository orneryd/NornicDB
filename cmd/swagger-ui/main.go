// Package main provides a standalone Swagger UI server for testing and validating the OpenAPI specification.
//
// This server serves the Swagger UI interface and the OpenAPI spec file, allowing developers
// to interactively test and validate all NornicDB API endpoints.
//
// Usage:
//
//	go run cmd/swagger-ui/main.go
//
// Or build and run:
//
//	make build-swagger-ui
//	./bin/swagger-ui
//
// The server will start on http://localhost:8080 by default.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"html/template"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
)

const (
	defaultPort = 8080
)

var swaggerUIPageTemplate = template.Must(template.New("swagger-ui-page").Parse(`<!DOCTYPE html>
<html lang="en">
<head>
	<meta charset="UTF-8">
	<meta name="viewport" content="width=device-width, initial-scale=1.0">
	<title>NornicDB API Documentation - Swagger UI</title>
	<link rel="stylesheet" type="text/css" href="https://unpkg.com/swagger-ui-dist@5.10.3/swagger-ui.css" />
	<style>
		html {
			box-sizing: border-box;
			overflow: -moz-scrollbars-vertical;
			overflow-y: scroll;
		}
		*, *:before, *:after {
			box-sizing: inherit;
		}
		body {
			margin:0;
			background: #fafafa;
		}
		.swagger-ui .topbar {
			background-color: #1f2937;
		}
		.swagger-ui .topbar .download-url-wrapper {
			display: none;
		}
	</style>
</head>
<body>
	<div id="swagger-ui"></div>
	<script src="https://unpkg.com/swagger-ui-dist@5.10.3/swagger-ui-bundle.js"></script>
	<script src="https://unpkg.com/swagger-ui-dist@5.10.3/swagger-ui-standalone-preset.js"></script>
	<script>
		window.onload = function() {
			const ui = SwaggerUIBundle({
	      url: {{ .SpecURLJSON }},
				dom_id: '#swagger-ui',
				deepLinking: true,
				presets: [
					SwaggerUIBundle.presets.apis,
					SwaggerUIStandalonePreset
				],
				plugins: [
					SwaggerUIBundle.plugins.DownloadUrl
				],
				layout: "StandaloneLayout",
				validatorUrl: null,
				tryItOutEnabled: true,
				supportedSubmitMethods: ['get', 'post', 'put', 'delete', 'patch'],
				onComplete: function() {
					const servers = ui.getSystem().specSelectors.specJson().get('servers');
					if (servers && servers.size > 0) {
						console.log('Swagger UI loaded. Configure server URL in the top-right dropdown.');
					}
				}
			});

			console.log('Tip: Configure the server URL in the top-right dropdown to point to your NornicDB instance (default: http://localhost:7474)');
		};
	</script>
</body>
</html>`))

func main() {
	port := flag.Int("port", defaultPort, "Port to listen on")
	flag.Parse()

	// Read the OpenAPI spec from filesystem
	// Try multiple possible paths
	var specData []byte
	var err error
	var specPath string

	// Get current working directory
	wd, _ := os.Getwd()

	// Try different paths
	possiblePaths := []string{
		"docs/api-reference/openapi.yaml",                                // From project root
		filepath.Join("..", "docs", "api-reference", "openapi.yaml"),     // From cmd/swagger-ui
		filepath.Join(wd, "docs", "api-reference", "openapi.yaml"),       // Absolute from project root
		filepath.Join(wd, "..", "docs", "api-reference", "openapi.yaml"), // Absolute from cmd/swagger-ui
	}

	for _, path := range possiblePaths {
		specData, err = os.ReadFile(path)
		if err == nil {
			specPath = path
			break
		}
	}

	if err != nil {
		log.Fatalf("Failed to read OpenAPI spec. Tried paths:\n"+
			"  - docs/api-reference/openapi.yaml\n"+
			"  - ../docs/api-reference/openapi.yaml\n"+
			"  - %s/docs/api-reference/openapi.yaml\n"+
			"  - %s/../docs/api-reference/openapi.yaml\n"+
			"Current working directory: %s\n"+
			"Error: %v", wd, wd, wd, err)
	}

	log.Printf("Loaded OpenAPI spec from: %s", specPath)

	// Create HTTP handlers
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		serveSwaggerUI(w, r)
	})

	http.HandleFunc("/swagger", serveSwaggerUI)
	http.HandleFunc("/swagger/", serveSwaggerUI)
	http.HandleFunc("/api-docs", serveSwaggerUI)
	http.HandleFunc("/api-docs/", serveSwaggerUI)

	http.HandleFunc("/openapi.yaml", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/yaml")
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.WriteHeader(http.StatusOK)
		w.Write(specData)
	})

	http.HandleFunc("/openapi.yml", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/yaml")
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.WriteHeader(http.StatusOK)
		w.Write(specData)
	})

	addr := fmt.Sprintf(":%d", *port)
	log.Printf("\n🚀 Swagger UI server running on http://localhost%s", addr)
	log.Printf("\n📚 Endpoints:")
	log.Printf("   • Swagger UI:  http://localhost%s/swagger", addr)
	log.Printf("   • OpenAPI Spec: http://localhost%s/openapi.yaml", addr)
	log.Printf("\n💡 Tips:")
	log.Printf("   • Use the Swagger UI to test all NornicDB API endpoints")
	log.Printf("   • Configure the server URL in Swagger UI to point to your NornicDB instance")
	log.Printf("   • Default NornicDB URL: http://localhost:7474")
	log.Printf("\n✅ Ready to test API endpoints!\n")

	if err := http.ListenAndServe(addr, nil); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}

// serveSwaggerUI serves the Swagger UI HTML page
func serveSwaggerUI(w http.ResponseWriter, r *http.Request) {
	// Determine the base URL for the OpenAPI spec
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	// Sanitize host header to prevent XSS
	host := r.Host
	if host == "" {
		host = "localhost:8080"
	}
	// Remove any potentially dangerous characters from host
	host = sanitizeHost(host)
	specURL := fmt.Sprintf("%s://%s/openapi.yaml", scheme, host)
	specURLJSON, err := json.Marshal(specURL)
	if err != nil {
		http.Error(w, "failed to encode Swagger spec URL", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	if err := swaggerUIPageTemplate.Execute(w, struct{ SpecURLJSON template.JS }{SpecURLJSON: template.JS(specURLJSON)}); err != nil {
		http.Error(w, "failed to render Swagger UI", http.StatusInternalServerError)
	}
}

// sanitizeHost removes potentially dangerous characters from host header
func sanitizeHost(host string) string {
	// Remove any characters that could be used for XSS
	host = strings.ReplaceAll(host, "<", "")
	host = strings.ReplaceAll(host, ">", "")
	host = strings.ReplaceAll(host, "\"", "")
	host = strings.ReplaceAll(host, "'", "")
	host = strings.ReplaceAll(host, "&", "")
	host = strings.ReplaceAll(host, ";", "")
	host = strings.ReplaceAll(host, "(", "")
	host = strings.ReplaceAll(host, ")", "")
	host = strings.ReplaceAll(host, "{", "")
	host = strings.ReplaceAll(host, "}", "")
	host = strings.ReplaceAll(host, "[", "")
	host = strings.ReplaceAll(host, "]", "")
	// Only allow alphanumeric, dots, colons, dashes, and underscores
	var sanitized strings.Builder
	for _, r := range host {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') ||
			(r >= '0' && r <= '9') || r == '.' || r == ':' || r == '-' || r == '_' {
			sanitized.WriteRune(r)
		}
	}
	result := sanitized.String()
	if result == "" {
		return "localhost:8080"
	}
	return result
}
