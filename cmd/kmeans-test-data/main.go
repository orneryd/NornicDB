// K-Means Test Data Generator for NornicDB
//
// This tool generates synthetic embeddings or downloads real datasets for testing
// the k-means clustering functionality in NornicDB's search service.
//
// Usage:
//
//	go run cmd/kmeans-test-data/main.go [options]
//
// Options:
//
//	-mode       Generation mode: synthetic, clusters, download, movies (default: clusters)
//	-count      Number of embeddings to generate (default: 5000)
//	-dims       Embedding dimensions (default: 1024)
//	-clusters   Number of natural clusters for 'clusters' mode (default: 20)
//	-output     Output directory for generated data (default: ./data/kmeans-test)
//	-db         NornicDB data directory to import into (if set, imports directly)
//	-dataset    Dataset to download: text-1024, movies-wiki, movies-tmdb
//
// Examples:
//
//	# Generate 5000 random embeddings
//	go run cmd/kmeans-test-data/main.go -mode synthetic -count 5000
//
//	# Generate 10000 embeddings with 50 natural clusters (best for k-means testing)
//	go run cmd/kmeans-test-data/main.go -mode clusters -count 10000 -clusters 50
//
//	# Download Wikipedia movie plots dataset
//	go run cmd/kmeans-test-data/main.go -mode movies -dataset movies-wiki
//
//	# Import directly into NornicDB
//	go run cmd/kmeans-test-data/main.go -mode clusters -count 5000 -db ./data/nornicdb
package main

import (
	"compress/gzip"
	"context"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// Known dataset URLs
const (
	// Wikipedia Movie Plots - ~35K movies with full plot summaries
	// Source: https://www.kaggle.com/datasets/jrobischon/wikipedia-movie-plots
	WikiMoviePlotsURL = "https://raw.githubusercontent.com/rfordatascience/tidytuesday/master/data/2018/2018-10-23/movie_profit.csv"

	// TMDB 5000 Movies - Popular movies with overviews
	// Alternative mirror for testing
	TMDB5000URL = "https://raw.githubusercontent.com/danielgrijalva/movie-stats/master/movies.csv"
)

func main() {
	// Parse flags
	mode := flag.String("mode", "clusters", "Generation mode: synthetic, clusters, download, movies")
	count := flag.Int("count", 5000, "Number of embeddings to generate")
	dims := flag.Int("dims", 1024, "Embedding dimensions")
	numClusters := flag.Int("clusters", 20, "Number of natural clusters (for clusters mode)")
	outputDir := flag.String("output", "./data/kmeans-test", "Output directory")
	dbDir := flag.String("db", "", "NornicDB data directory (if set, imports directly)")
	dataset := flag.String("dataset", "", "Dataset: text-1024, movies-sample, movies-tmdb, movies-wiki (default depends on mode)")
	seed := flag.Int64("seed", 42, "Random seed for reproducibility")
	flag.Parse()

	rand.Seed(*seed)

	// Set default dataset based on mode
	effectiveDataset := *dataset
	if effectiveDataset == "" {
		switch *mode {
		case "movies":
			effectiveDataset = "movies-sample"
		case "download":
			effectiveDataset = "text-1024"
		}
	}

	log.Printf("🧪 K-Means Test Data Generator")
	log.Printf("   Mode: %s", *mode)
	log.Printf("   Seed: %d", *seed)

	var embeddings []TestEmbedding
	var textData []TextData
	var err error

	switch *mode {
	case "synthetic":
		log.Printf("   Count: %d", *count)
		log.Printf("   Dimensions: %d", *dims)
		log.Printf("📊 Generating %d random embeddings...", *count)
		embeddings = generateRandomEmbeddings(*count, *dims)

	case "clusters":
		log.Printf("   Count: %d", *count)
		log.Printf("   Dimensions: %d", *dims)
		log.Printf("📊 Generating %d embeddings with %d natural clusters...", *count, *numClusters)
		embeddings = generateClusteredEmbeddings(*count, *dims, *numClusters)

	case "download":
		log.Printf("   Dataset: %s", effectiveDataset)
		log.Printf("📥 Generating %s dataset...", effectiveDataset)
		embeddings, err = generateDataset(effectiveDataset)
		if err != nil {
			log.Fatalf("Generation failed: %v", err)
		}

	case "movies":
		log.Printf("   Dataset: %s", effectiveDataset)
		log.Printf("📥 Downloading movie dataset: %s...", effectiveDataset)
		textData, err = downloadMovieDataset(effectiveDataset, *outputDir, *count)
		if err != nil {
			log.Fatalf("Download failed: %v", err)
		}
		log.Printf("✅ Downloaded %d movies with text content", len(textData))

		// Save text data for embedding later
		if *dbDir != "" {
			log.Printf("📤 Importing into NornicDB at %s...", *dbDir)
			if err := importTextToStorage(textData, *dbDir); err != nil {
				log.Fatalf("Import failed: %v", err)
			}
			log.Printf("✅ Successfully imported %d movies to NornicDB", len(textData))
			log.Printf("")
			log.Printf("📝 These nodes need embeddings generated.")
			log.Printf("   Start NornicDB with an embedder to auto-generate embeddings:")
			log.Printf("   NORNICDB_KMEANS_CLUSTERING_ENABLED=true go run cmd/nornicdb/main.go -data %s", *dbDir)
		} else {
			// Save to JSON
			if err := os.MkdirAll(*outputDir, 0755); err != nil {
				log.Fatalf("Failed to create output directory: %v", err)
			}
			outputFile := filepath.Join(*outputDir, fmt.Sprintf("movies_%s_%d.json", effectiveDataset, len(textData)))
			if err := saveTextData(textData, outputFile); err != nil {
				log.Fatalf("Failed to save: %v", err)
			}
			log.Printf("✅ Saved to %s", outputFile)
		}
		printTextStats(textData)
		return

	default:
		log.Fatalf("Unknown mode: %s", *mode)
	}

	log.Printf("✅ Generated %d embeddings", len(embeddings))

	// Either import to DB or save to file
	if *dbDir != "" {
		log.Printf("📤 Importing into NornicDB at %s...", *dbDir)
		if err := importToStorage(embeddings, *dbDir); err != nil {
			log.Fatalf("Import failed: %v", err)
		}
		log.Printf("✅ Successfully imported %d embeddings to NornicDB", len(embeddings))
		log.Printf("")
		log.Printf("🔬 To test k-means clustering:")
		log.Printf("   export NORNICDB_KMEANS_CLUSTERING_ENABLED=true")
		log.Printf("   go run cmd/nornicdb/main.go -data %s", *dbDir)
	} else {
		// Save to JSON file
		if err := os.MkdirAll(*outputDir, 0755); err != nil {
			log.Fatalf("Failed to create output directory: %v", err)
		}
		outputFile := filepath.Join(*outputDir, fmt.Sprintf("embeddings_%s_%d.json", *mode, *count))
		if err := saveEmbeddings(embeddings, outputFile); err != nil {
			log.Fatalf("Failed to save embeddings: %v", err)
		}
		log.Printf("✅ Saved to %s", outputFile)
	}

	// Print statistics
	printStats(embeddings)
}

// TestEmbedding represents a test embedding with metadata
type TestEmbedding struct {
	ID        string    `json:"id"`
	Label     string    `json:"label"`
	ClusterID int       `json:"cluster_id,omitempty"` // For clustered mode
	Embedding []float32 `json:"embedding"`
}

// TextData represents text content that needs to be embedded
type TextData struct {
	ID         string `json:"id"`
	Title      string `json:"title"`
	Content    string `json:"content"`
	Category   string `json:"category,omitempty"`
	Year       int    `json:"year,omitempty"`
	Genre      string `json:"genre,omitempty"`
	ContentLen int    `json:"content_len"`
}

// generateRandomEmbeddings generates uniformly distributed random embeddings
func generateRandomEmbeddings(count, dims int) []TestEmbedding {
	embeddings := make([]TestEmbedding, count)

	for i := 0; i < count; i++ {
		emb := make([]float32, dims)
		for j := 0; j < dims; j++ {
			emb[j] = rand.Float32()*2 - 1 // [-1, 1]
		}
		normalize(emb)

		embeddings[i] = TestEmbedding{
			ID:        fmt.Sprintf("test-%06d", i),
			Label:     "TestData",
			Embedding: emb,
		}

		if (i+1)%1000 == 0 {
			log.Printf("   Generated %d/%d...", i+1, count)
		}
	}

	return embeddings
}

// generateClusteredEmbeddings generates embeddings with natural cluster structure
func generateClusteredEmbeddings(count, dims, numClusters int) []TestEmbedding {
	embeddings := make([]TestEmbedding, count)

	// Generate cluster centroids
	centroids := make([][]float32, numClusters)
	for i := 0; i < numClusters; i++ {
		centroid := make([]float32, dims)
		for j := 0; j < dims; j++ {
			centroid[j] = rand.Float32()*2 - 1
		}
		normalize(centroid)
		centroids[i] = centroid
	}

	// Generate points around centroids
	pointsPerCluster := count / numClusters
	stdDev := float32(0.15)

	idx := 0
	for clusterID := 0; clusterID < numClusters; clusterID++ {
		centroid := centroids[clusterID]
		clusterSize := pointsPerCluster
		if clusterID == numClusters-1 {
			clusterSize = count - idx
		}

		for i := 0; i < clusterSize && idx < count; i++ {
			emb := make([]float32, dims)
			for j := 0; j < dims; j++ {
				noise := float32(rand.NormFloat64()) * stdDev
				emb[j] = centroid[j] + noise
			}
			normalize(emb)

			embeddings[idx] = TestEmbedding{
				ID:        fmt.Sprintf("test-%06d", idx),
				Label:     fmt.Sprintf("Cluster%d", clusterID),
				ClusterID: clusterID,
				Embedding: emb,
			}
			idx++
		}

		if (clusterID+1)%5 == 0 {
			log.Printf("   Generated cluster %d/%d...", clusterID+1, numClusters)
		}
	}

	rand.Shuffle(len(embeddings), func(i, j int) {
		embeddings[i], embeddings[j] = embeddings[j], embeddings[i]
	})

	return embeddings
}

// generateDataset generates a pseudo-real embedding dataset
func generateDataset(name string) ([]TestEmbedding, error) {
	switch name {
	case "sift-small":
		log.Printf("📊 Generating SIFT-like data (128 dims, 10000 vectors)...")
		return generateClusteredEmbeddings(10000, 128, 100), nil

	case "glove-25":
		log.Printf("📊 Generating GloVe-like data (25 dims, 10000 vectors)...")
		return generateClusteredEmbeddings(10000, 25, 50), nil

	case "text-1024":
		log.Printf("📊 Generating text embedding data (1024 dims, 10000 vectors)...")
		return generateClusteredEmbeddings(10000, 1024, 100), nil

	case "large-text":
		log.Printf("📊 Generating large text embedding data (1024 dims, 50000 vectors)...")
		return generateClusteredEmbeddings(50000, 1024, 200), nil

	default:
		return nil, fmt.Errorf("unknown dataset: %s", name)
	}
}

// downloadMovieDataset downloads a movie dataset from the internet
func downloadMovieDataset(dataset string, outputDir string, maxCount int) ([]TextData, error) {
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return nil, err
	}

	switch dataset {
	case "movies-sample":
		// Generate a sample movie dataset for testing
		return generateSampleMovies(maxCount), nil

	case "movies-tmdb":
		return downloadTMDBMovies(outputDir, maxCount)

	case "movies-wiki":
		return downloadWikiMovies(outputDir, maxCount)

	default:
		return nil, fmt.Errorf("unknown movie dataset: %s (available: movies-sample, movies-tmdb, movies-wiki)", dataset)
	}
}

// generateSampleMovies creates a sample movie dataset with rich semantic content
// The content is designed to produce meaningful embeddings that will cluster by genre
func generateSampleMovies(count int) []TextData {
	// Genre-specific vocabulary for meaningful semantic clustering
	genreData := map[string]struct {
		settings   []string
		characters []string
		plots      []string
		themes     []string
		keywords   []string
	}{
		"Action": {
			settings:   []string{"war-torn city", "explosive battlefield", "high-speed chase through downtown", "military compound", "burning skyscraper"},
			characters: []string{"elite soldier", "martial arts master", "rogue agent", "fearless mercenary", "trained assassin"},
			plots:      []string{"must stop a terrorist attack", "races against time to defuse bombs", "fights an army of enemies", "escapes from maximum security prison", "battles criminal syndicate"},
			themes:     []string{"courage under fire", "sacrifice for country", "brotherhood in combat", "revenge and justice", "survival against odds"},
			keywords:   []string{"explosions", "gunfights", "hand-to-hand combat", "car chases", "military operations"},
		},
		"Comedy": {
			settings:   []string{"chaotic wedding venue", "dysfunctional family reunion", "absurd workplace", "wild party gone wrong", "awkward first date"},
			characters: []string{"bumbling idiot", "sarcastic best friend", "eccentric relative", "uptight boss", "lovable loser"},
			plots:      []string{"accidentally ruins everything", "gets into hilarious misunderstandings", "pretends to be someone else", "tries to impress their crush", "attempts an outrageous scheme"},
			themes:     []string{"friendship and laughter", "embracing imperfection", "finding joy in chaos", "being yourself", "unlikely friendships"},
			keywords:   []string{"slapstick", "witty banter", "embarrassing situations", "comedic timing", "laugh-out-loud moments"},
		},
		"Drama": {
			settings:   []string{"struggling family home", "intense courtroom", "hospital ward", "poverty-stricken neighborhood", "prestigious institution"},
			characters: []string{"grieving widow", "ambitious lawyer", "troubled teenager", "dedicated teacher", "recovering addict"},
			plots:      []string{"confronts painful past", "fights for custody of children", "overcomes personal demons", "seeks redemption for mistakes", "struggles with terminal illness"},
			themes:     []string{"human resilience", "family bonds", "moral dilemmas", "social inequality", "personal growth"},
			keywords:   []string{"emotional depth", "powerful performances", "heart-wrenching scenes", "character development", "authentic storytelling"},
		},
		"Horror": {
			settings:   []string{"haunted mansion", "isolated cabin in the woods", "abandoned asylum", "cursed cemetery", "creepy small town"},
			characters: []string{"terrified victim", "paranormal investigator", "possessed child", "vengeful spirit", "masked killer"},
			plots:      []string{"uncovers dark secrets of the past", "fights to survive the night", "confronts supernatural evil", "escapes from nightmare creature", "breaks an ancient curse"},
			themes:     []string{"facing your fears", "survival horror", "supernatural terror", "psychological dread", "evil awakening"},
			keywords:   []string{"jump scares", "suspenseful atmosphere", "gore and blood", "demonic possession", "nightmare sequences"},
		},
		"Sci-Fi": {
			settings:   []string{"distant planet colony", "space station orbiting earth", "dystopian future city", "underground bunker", "alien spacecraft"},
			characters: []string{"space explorer", "artificial intelligence", "genetically enhanced human", "alien ambassador", "rebel scientist"},
			plots:      []string{"discovers alien civilization", "fights against machine uprising", "travels through time paradox", "searches for habitable planet", "uncovers government conspiracy"},
			themes:     []string{"humanity's future", "artificial consciousness", "space exploration", "technological ethics", "first contact"},
			keywords:   []string{"futuristic technology", "space travel", "parallel dimensions", "genetic engineering", "robot companions"},
		},
		"Romance": {
			settings:   []string{"charming coffee shop", "romantic Paris streets", "beautiful seaside town", "glamorous New York apartment", "rustic countryside vineyard"},
			characters: []string{"hopeless romantic", "cynical divorcee", "childhood sweetheart", "mysterious stranger", "best friend turned lover"},
			plots:      []string{"falls for unexpected person", "reunites with lost love", "chooses between two lovers", "overcomes family disapproval", "finds love in unlikely place"},
			themes:     []string{"true love conquers all", "second chances at love", "forbidden romance", "soulmates finding each other", "love versus career"},
			keywords:   []string{"passionate kisses", "romantic gestures", "heartfelt confessions", "wedding bells", "love at first sight"},
		},
		"Thriller": {
			settings:   []string{"shadowy corporate office", "dangerous underground club", "tense hostage situation", "psychological ward", "isolated research facility"},
			characters: []string{"cunning detective", "unreliable narrator", "manipulative psychopath", "innocent suspect", "obsessed stalker"},
			plots:      []string{"races to catch serial killer", "uncovers deadly conspiracy", "escapes from kidnapper", "solves impossible murder", "protects family from danger"},
			themes:     []string{"trust and betrayal", "cat and mouse games", "hidden identities", "paranoia and suspicion", "justice versus revenge"},
			keywords:   []string{"plot twists", "edge-of-seat tension", "psychological manipulation", "deadly games", "shocking revelations"},
		},
		"Documentary": {
			settings:   []string{"remote wildlife sanctuary", "war-torn region", "inner city streets", "prestigious university", "environmental disaster zone"},
			characters: []string{"passionate activist", "renowned scientist", "ordinary hero", "controversial figure", "indigenous community"},
			plots:      []string{"exposes hidden truth", "follows remarkable journey", "investigates unsolved mystery", "documents historical event", "reveals systemic injustice"},
			themes:     []string{"truth and justice", "environmental awareness", "social change", "human achievement", "preserving history"},
			keywords:   []string{"real-life stories", "investigative journalism", "interviews and archives", "factual narrative", "eye-opening revelations"},
		},
		"Animation": {
			settings:   []string{"magical kingdom", "underwater world", "enchanted forest", "toy-filled bedroom", "fantastical dreamscape"},
			characters: []string{"brave princess", "talking animal sidekick", "evil sorcerer", "misunderstood monster", "heroic robot"},
			plots:      []string{"embarks on magical quest", "saves kingdom from darkness", "learns important life lesson", "befriends unlikely companion", "discovers hidden powers"},
			themes:     []string{"friendship and loyalty", "believing in yourself", "family love", "good versus evil", "imagination and wonder"},
			keywords:   []string{"colorful animation", "musical numbers", "family-friendly", "beloved characters", "magical adventures"},
		},
		"Adventure": {
			settings:   []string{"ancient temple ruins", "treacherous mountain peak", "uncharted jungle", "mysterious island", "vast desert wasteland"},
			characters: []string{"treasure hunter", "fearless explorer", "ship captain", "archaeological professor", "survival expert"},
			plots:      []string{"searches for legendary artifact", "discovers lost civilization", "survives in hostile wilderness", "races rivals to treasure", "maps uncharted territory"},
			themes:     []string{"discovery and wonder", "courage and determination", "nature's power", "human endurance", "historical mysteries"},
			keywords:   []string{"epic journeys", "exotic locations", "dangerous obstacles", "ancient secrets", "breathtaking landscapes"},
		},
	}

	genres := make([]string, 0, len(genreData))
	for g := range genreData {
		genres = append(genres, g)
	}

	movies := make([]TextData, count)
	for i := 0; i < count; i++ {
		genre := genres[rand.Intn(len(genres))]
		gd := genreData[genre]

		// Build a rich, genre-specific plot description
		setting := gd.settings[rand.Intn(len(gd.settings))]
		character := gd.characters[rand.Intn(len(gd.characters))]
		plotAction := gd.plots[rand.Intn(len(gd.plots))]
		theme := gd.themes[rand.Intn(len(gd.themes))]
		keyword1 := gd.keywords[rand.Intn(len(gd.keywords))]
		keyword2 := gd.keywords[rand.Intn(len(gd.keywords))]

		// Create semantically rich content that will cluster by genre
		plot := fmt.Sprintf("Set in a %s, this %s film follows a %s who %s. ",
			setting, strings.ToLower(genre), character, plotAction)
		plot += fmt.Sprintf("The story explores themes of %s through %s and %s. ",
			theme, keyword1, keyword2)
		plot += fmt.Sprintf("A %s character study with %s, this movie features %s. ",
			strings.ToLower(genre), gd.themes[rand.Intn(len(gd.themes))], gd.keywords[rand.Intn(len(gd.keywords))])
		plot += fmt.Sprintf("Perfect for fans of %s who enjoy %s and %s.",
			strings.ToLower(genre), gd.keywords[rand.Intn(len(gd.keywords))], gd.settings[rand.Intn(len(gd.settings))])

		// Genre-specific title prefixes
		titlePrefixes := map[string][]string{
			"Action":      {"Strike Force", "Combat Zone", "Lethal", "Deadly", "Extreme"},
			"Comedy":      {"Crazy", "Hilarious", "Madcap", "Wacky", "Ridiculous"},
			"Drama":       {"Profound", "Touching", "Heartfelt", "Emotional", "Moving"},
			"Horror":      {"Nightmare", "Terror", "Haunted", "Cursed", "Evil"},
			"Sci-Fi":      {"Galactic", "Quantum", "Cyber", "Stellar", "Cosmic"},
			"Romance":     {"Eternal", "Passionate", "Beloved", "Enchanted", "Devoted"},
			"Thriller":    {"Deadly", "Twisted", "Dark", "Dangerous", "Lethal"},
			"Documentary": {"Revealing", "Truth Behind", "Inside", "Exposing", "Uncovering"},
			"Animation":   {"Magical", "Enchanted", "Wonderful", "Amazing", "Fantastic"},
			"Adventure":   {"Epic", "Quest for", "Journey to", "Expedition", "Voyage of"},
		}

		prefixes := titlePrefixes[genre]
		prefix := prefixes[rand.Intn(len(prefixes))]
		suffixes := []string{"Legacy", "Chronicles", "Saga", "Story", "Tales", "Mission", "Secret", "Destiny"}

		movies[i] = TextData{
			ID:         fmt.Sprintf("movie-%05d", i),
			Title:      fmt.Sprintf("%s %s", prefix, suffixes[rand.Intn(len(suffixes))]),
			Content:    plot,
			Category:   "Movie",
			Year:       1980 + rand.Intn(45),
			Genre:      genre,
			ContentLen: len(plot),
		}

		if (i+1)%500 == 0 {
			log.Printf("   Generated %d/%d movies...", i+1, count)
		}
	}

	return movies
}

// downloadTMDBMovies downloads movies from TMDB dataset mirror
func downloadTMDBMovies(outputDir string, maxCount int) ([]TextData, error) {
	log.Printf("📥 Downloading TMDB movies dataset...")

	cacheFile := filepath.Join(outputDir, "tmdb_movies.csv")

	// Check if cached
	if _, err := os.Stat(cacheFile); os.IsNotExist(err) {
		log.Printf("   Fetching from %s", TMDB5000URL)
		if err := downloadFile(TMDB5000URL, cacheFile); err != nil {
			log.Printf("   ⚠️  Download failed: %v", err)
			log.Printf("   Falling back to sample data...")
			return generateSampleMovies(maxCount), nil
		}
	} else {
		log.Printf("   Using cached file: %s", cacheFile)
	}

	// Parse CSV
	return parseTMDBCSV(cacheFile, maxCount)
}

// downloadWikiMovies downloads Wikipedia movie plots
func downloadWikiMovies(outputDir string, maxCount int) ([]TextData, error) {
	log.Printf("📥 Downloading Wikipedia movie plots...")

	// Since the full Wiki dataset requires Kaggle auth, use sample data
	log.Printf("   Note: Full Wikipedia dataset requires Kaggle authentication")
	log.Printf("   Generating comprehensive sample dataset instead...")

	return generateSampleMovies(maxCount), nil
}

// parseTMDBCSV parses the TMDB CSV file
func parseTMDBCSV(filename string, maxCount int) ([]TextData, error) {
	// Sanitize path to prevent path traversal attacks
	cleanPath := filepath.Clean(filename)
	if strings.Contains(cleanPath, "..") {
		return nil, fmt.Errorf("invalid path: directory traversal not allowed")
	}
	file, err := os.Open(cleanPath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.LazyQuotes = true
	reader.FieldsPerRecord = -1 // Variable fields

	// Read header
	header, err := reader.Read()
	if err != nil {
		return nil, err
	}

	// Find column indices
	nameIdx, overviewIdx, genreIdx, yearIdx := -1, -1, -1, -1
	for i, col := range header {
		switch strings.ToLower(strings.TrimSpace(col)) {
		case "name", "title", "original_title":
			nameIdx = i
		case "overview", "plot", "description":
			overviewIdx = i
		case "genre", "genres":
			genreIdx = i
		case "year", "release_year", "released":
			yearIdx = i
		}
	}

	if nameIdx == -1 {
		nameIdx = 0 // Fallback to first column
	}

	var movies []TextData
	for i := 0; len(movies) < maxCount; i++ {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			continue // Skip bad records
		}

		title := ""
		if nameIdx < len(record) {
			title = strings.TrimSpace(record[nameIdx])
		}
		if title == "" {
			continue
		}

		overview := ""
		if overviewIdx >= 0 && overviewIdx < len(record) {
			overview = strings.TrimSpace(record[overviewIdx])
		}
		if overview == "" || len(overview) < 20 {
			continue // Skip movies without descriptions
		}

		genre := "Unknown"
		if genreIdx >= 0 && genreIdx < len(record) {
			genre = strings.TrimSpace(record[genreIdx])
		}

		year := 2000
		if yearIdx >= 0 && yearIdx < len(record) {
			fmt.Sscanf(record[yearIdx], "%d", &year)
		}

		movies = append(movies, TextData{
			ID:         fmt.Sprintf("movie-%05d", len(movies)),
			Title:      title,
			Content:    overview,
			Category:   "Movie",
			Year:       year,
			Genre:      genre,
			ContentLen: len(overview),
		})

		if len(movies)%500 == 0 {
			log.Printf("   Parsed %d movies...", len(movies))
		}
	}

	return movies, nil
}

// downloadFile downloads a file from URL
func downloadFile(url string, filepath string) error {
	client := &http.Client{
		Timeout: 60 * time.Second,
	}

	resp, err := client.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, resp.Status)
	}

	out, err := os.Create(filepath)
	if err != nil {
		return err
	}
	defer out.Close()

	var reader io.Reader = resp.Body
	if strings.HasSuffix(url, ".gz") {
		reader, err = gzip.NewReader(resp.Body)
		if err != nil {
			return err
		}
	}

	_, err = io.Copy(out, reader)
	return err
}

// importTextToStorage imports text data into storage (without embeddings)
func importTextToStorage(textData []TextData, dbDir string) error {
	if err := os.MkdirAll(dbDir, 0755); err != nil {
		return fmt.Errorf("failed to create data directory: %w", err)
	}

	engine, err := storage.NewBadgerEngine(dbDir)
	if err != nil {
		return fmt.Errorf("failed to open storage: %w", err)
	}
	defer engine.Close()

	batchSize := 100
	startTime := time.Now()

	for i := 0; i < len(textData); i += batchSize {
		end := i + batchSize
		if end > len(textData) {
			end = len(textData)
		}

		for _, td := range textData[i:end] {
			node := &storage.Node{
				ID:     storage.NodeID(td.ID),
				Labels: []string{"Movie", td.Genre},
				Properties: map[string]interface{}{
					"title":      td.Title,
					"content":    td.Content,
					"year":       td.Year,
					"genre":      td.Genre,
					"imported":   true,
					"created_at": time.Now().Unix(),
				},
				// No embedding yet - will be generated by auto-embed queue
			}

			if err := engine.CreateNode(node); err != nil {
				log.Printf("Warning: failed to create node %s: %v", td.ID, err)
			}
		}

		if (i+batchSize)%500 == 0 || end == len(textData) {
			elapsed := time.Since(startTime)
			rate := float64(end) / elapsed.Seconds()
			log.Printf("   Imported %d/%d (%.0f nodes/sec)...", end, len(textData), rate)
		}
	}

	return nil
}

// importToStorage imports embeddings directly using the storage layer
func importToStorage(embeddings []TestEmbedding, dbDir string) error {
	if err := os.MkdirAll(dbDir, 0755); err != nil {
		return fmt.Errorf("failed to create data directory: %w", err)
	}

	engine, err := storage.NewBadgerEngine(dbDir)
	if err != nil {
		return fmt.Errorf("failed to open storage: %w", err)
	}
	defer engine.Close()

	ctx := context.Background()
	_ = ctx

	batchSize := 100
	startTime := time.Now()

	for i := 0; i < len(embeddings); i += batchSize {
		end := i + batchSize
		if end > len(embeddings) {
			end = len(embeddings)
		}

		for _, emb := range embeddings[i:end] {
			node := &storage.Node{
				ID:     storage.NodeID(emb.ID),
				Labels: []string{emb.Label, "TestEmbedding"},
				Properties: map[string]interface{}{
					"cluster_id": emb.ClusterID,
					"generated":  true,
					"created_at": time.Now().Unix(),
				},
				Embedding: emb.Embedding,
			}

			if err := engine.CreateNode(node); err != nil {
				log.Printf("Warning: failed to create node %s: %v", emb.ID, err)
			}
		}

		if (i+batchSize)%1000 == 0 || end == len(embeddings) {
			elapsed := time.Since(startTime)
			rate := float64(end) / elapsed.Seconds()
			log.Printf("   Imported %d/%d (%.0f nodes/sec)...", end, len(embeddings), rate)
		}
	}

	return nil
}

// saveEmbeddings saves embeddings to a JSON file
func saveEmbeddings(embeddings []TestEmbedding, filename string) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	return encoder.Encode(embeddings)
}

// saveTextData saves text data to a JSON file
func saveTextData(data []TextData, filename string) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	return encoder.Encode(data)
}

// normalize normalizes a vector to unit length
func normalize(v []float32) {
	var sum float64
	for _, x := range v {
		sum += float64(x * x)
	}
	norm := float32(math.Sqrt(sum))
	if norm > 0 {
		for i := range v {
			v[i] /= norm
		}
	}
}

// printStats prints statistics about the generated embeddings
func printStats(embeddings []TestEmbedding) {
	if len(embeddings) == 0 {
		return
	}

	dims := len(embeddings[0].Embedding)
	clusters := make(map[int]int)
	for _, emb := range embeddings {
		clusters[emb.ClusterID]++
	}

	log.Printf("")
	log.Printf("📈 Statistics:")
	log.Printf("   Total embeddings: %d", len(embeddings))
	log.Printf("   Dimensions: %d", dims)
	log.Printf("   Unique clusters: %d", len(clusters))

	if len(clusters) > 1 {
		minSize, maxSize := len(embeddings), 0
		for _, size := range clusters {
			if size < minSize {
				minSize = size
			}
			if size > maxSize {
				maxSize = size
			}
		}
		avgSize := float64(len(embeddings)) / float64(len(clusters))
		log.Printf("   Cluster sizes: min=%d max=%d avg=%.1f", minSize, maxSize, avgSize)
	}

	var sumNorm float64
	sampleSize := 100
	if sampleSize > len(embeddings) {
		sampleSize = len(embeddings)
	}
	for i := 0; i < sampleSize; i++ {
		var norm float64
		for _, x := range embeddings[i].Embedding {
			norm += float64(x * x)
		}
		sumNorm += math.Sqrt(norm)
	}
	avgNorm := sumNorm / float64(sampleSize)
	log.Printf("   Avg vector norm: %.4f (should be ~1.0)", avgNorm)

	memMB := float64(len(embeddings)*dims*4) / (1024 * 1024)
	log.Printf("   Memory usage: %.1f MB (embeddings only)", memMB)
}

// printTextStats prints statistics about text data
func printTextStats(data []TextData) {
	if len(data) == 0 {
		return
	}

	genres := make(map[string]int)
	totalLen := 0
	minLen, maxLen := data[0].ContentLen, data[0].ContentLen

	for _, d := range data {
		genres[d.Genre]++
		totalLen += d.ContentLen
		if d.ContentLen < minLen {
			minLen = d.ContentLen
		}
		if d.ContentLen > maxLen {
			maxLen = d.ContentLen
		}
	}

	log.Printf("")
	log.Printf("📈 Statistics:")
	log.Printf("   Total items: %d", len(data))
	log.Printf("   Unique genres: %d", len(genres))
	log.Printf("   Content length: min=%d max=%d avg=%d chars", minLen, maxLen, totalLen/len(data))

	// Top genres
	log.Printf("   Top genres:")
	type genreCount struct {
		name  string
		count int
	}
	var gc []genreCount
	for g, c := range genres {
		gc = append(gc, genreCount{g, c})
	}
	// Sort by count (simple bubble sort for small list)
	for i := 0; i < len(gc); i++ {
		for j := i + 1; j < len(gc); j++ {
			if gc[j].count > gc[i].count {
				gc[i], gc[j] = gc[j], gc[i]
			}
		}
	}
	for i := 0; i < 5 && i < len(gc); i++ {
		log.Printf("      %s: %d", gc[i].name, gc[i].count)
	}
}
