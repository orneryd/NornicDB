package server

import (
	"fmt"
	"os"
	"strconv"
)

func hsEnvI(name string, fallback int) int {
	value := os.Getenv(name)
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return parsed
}

func hsEnvF(name string, fallback float64) float64 {
	value := os.Getenv(name)
	if value == "" {
		return fallback
	}
	parsed, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return fallback
	}
	return parsed
}

func hsLog(format string, args ...any) {
	fmt.Printf("HS "+format+"\n", args...)
}
