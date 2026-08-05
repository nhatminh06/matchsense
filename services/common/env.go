package common

import "os"

// GetEnv returns the value of the given environment variable, or fallback
// if it is unset or empty.
func GetEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
