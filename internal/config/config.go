package config

import (
	"encoding/json"
	"errors"
	"os"
	"strings"
)

func Value(name string) (string, error) {
	if file := os.Getenv(name + "_FILE"); file != "" {
		data, err := os.ReadFile(file)
		if err != nil {
			return "", err
		}
		return strings.TrimSpace(string(data)), nil
	}
	value := os.Getenv(name)
	if value == "" {
		return "", errors.New(name + " or " + name + "_FILE is required")
	}
	return value, nil
}

func Tokens() (map[string]string, error) {
	raw, err := Value("API_TOKENS")
	if err != nil {
		return nil, err
	}
	var tokens map[string]string
	err = json.Unmarshal([]byte(raw), &tokens)
	return tokens, err
}
