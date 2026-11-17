package adapters

import (
	"strings"

	"github.com/drewv-labs/carrier-pigeons/pkg/adapters/db"
)

var _ BaseLLM = (*LlamaCPPAdapter)(nil)

// LlamaCPPAdapter wraps the OpenAI client to target a local llama.cpp server.
type LlamaCPPAdapter struct {
	*OpenAIAdapter
}

func NewLlamaCPPAdapter(endpoint string, store db.BaseDB) *LlamaCPPAdapter {
	if strings.TrimSpace(endpoint) == "" {
		endpoint = "http://localhost:8080/v1/chat/completions"
	}

	return &LlamaCPPAdapter{
		OpenAIAdapter: NewOpenAIAdapter(endpoint, "local-no-key", "default", store),
	}
}
