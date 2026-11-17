package adapters

import (
	"strings"

	"github.com/drewv-labs/carrier-pigeons/pkg/adapters/db"
)

var _ BaseLLM = (*VLLMAdapter)(nil)

// VLLMAdapter wraps the OpenAI client to target a local or edge vLLM cluster.
type VLLMAdapter struct {
	*OpenAIAdapter
}

func NewVLLMAdapter(endpoint, model string, store db.BaseDB) *VLLMAdapter {
	if strings.TrimSpace(endpoint) == "" {
		endpoint = "http://localhost:8000/v1/chat/completions"
	}
	if strings.TrimSpace(model) == "" {
		model = "Qwen/Qwen2.5-Coder-32B-Instruct"
	}

	return &VLLMAdapter{
		OpenAIAdapter: NewOpenAIAdapter(endpoint, "vllm-no-key", model, store),
	}
}
