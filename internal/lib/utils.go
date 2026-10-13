package lib

import "os"

func Getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	
	return def
	
}
