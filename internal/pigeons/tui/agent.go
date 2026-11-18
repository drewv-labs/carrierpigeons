package tui

import (
	"context"
	"strings"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/drewv-labs/carrier-pigeons/pkg/adapters/llm"
	"github.com/drewv-labs/carrier-pigeons/pkg/brand"
)

var (
	agentStyle  = lipgloss.NewStyle().Foreground(lipgloss.Color("205")).Bold(true)
	userStyle   = lipgloss.NewStyle().Foreground(lipgloss.Color("253"))
	systemStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("240")).BorderStyle(lipgloss.RoundedBorder()).Padding(0, 1)
	promptStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("86")).Bold(true)
	waitStyle   = lipgloss.NewStyle().Foreground(lipgloss.Color("240")).Italic(true)
)

type streamTokenMsg struct {
	token string
	ch    <-chan string
}
type streamDoneMsg struct{}
type streamErrMsg struct{ err error }

// AgentModel provides a standalone single-pane PigeonCare chat model when invoked directly.
type AgentModel struct {
	llm             llm.BaseLLM
	clusterContext  string
	chatHistory     []llm.Message
	uiHistory       []string
	userInput       string
	currentResponse string
	streamCancel    context.CancelFunc
	waiting         bool
	quitting        bool
}

func NewAgentModel(ctx string, adapter llm.BaseLLM) AgentModel {
	name := brand.Active().AgentName
	return AgentModel{
		llm:            adapter,
		clusterContext: ctx,
		uiHistory: []string{
			agentStyle.Render(name + ": I have analyzed the cluster topography. How can I assist with triage?"),
		},
	}
}

func (m AgentModel) Init() tea.Cmd {
	return nil
}

func (m AgentModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	name := brand.Active().AgentName
	switch msg := msg.(type) {

	case tea.KeyMsg:
		if msg.Type == tea.KeyCtrlC || msg.Type == tea.KeyEsc {
			if m.streamCancel != nil {
				m.streamCancel()
				m.streamCancel = nil
			}
			m.quitting = true
			return m, tea.Quit
		}

		if m.waiting {
			return m, nil
		}

		switch msg.Type {
		case tea.KeyEnter:
			if strings.TrimSpace(m.userInput) == "" {
				return m, nil
			}

			userText := m.userInput
			m.chatHistory = append(m.chatHistory, llm.Message{Role: "user", Content: userText})
			m.uiHistory = append(m.uiHistory, userStyle.Render("Operator: "+userText))
			m.uiHistory = append(m.uiHistory, agentStyle.Render(name+": "))

			m.userInput = ""
			m.currentResponse = ""
			m.waiting = true

			ctx, cancel := context.WithCancel(context.Background())
			m.streamCancel = cancel
			return m, m.startStream(ctx)

		case tea.KeyBackspace, tea.KeyDelete:
			runes := []rune(m.userInput)
			if len(runes) > 0 {
				m.userInput = string(runes[:len(runes)-1])
			}

		case tea.KeySpace:
			m.userInput += " "

		case tea.KeyRunes:
			m.userInput += string(msg.Runes)
		}

	case streamErrMsg:
		m.waiting = false
		if m.streamCancel != nil {
			m.streamCancel()
			m.streamCancel = nil
		}
		m.uiHistory[len(m.uiHistory)-1] = agentStyle.Render(name + " Error: " + msg.err.Error())
		return m, nil

	case streamTokenMsg:
		m.currentResponse += msg.token
		m.uiHistory[len(m.uiHistory)-1] = agentStyle.Render(name + ": " + m.currentResponse)
		return m, waitForToken(msg.ch)

	case streamDoneMsg:
		m.waiting = false
		if m.streamCancel != nil {
			m.streamCancel()
			m.streamCancel = nil
		}
		m.chatHistory = append(m.chatHistory, llm.Message{Role: "assistant", Content: m.currentResponse})
		return m, nil
	}

	return m, nil
}

func (m AgentModel) startStream(ctx context.Context) tea.Cmd {
	historySnapshot := append([]llm.Message(nil), m.chatHistory...)
	sysPrompt := "You are " + brand.Active().AgentName + ", an expert hardware site reliability engineer. Live cluster context:\n" + m.clusterContext
	adapter := m.llm

	return func() tea.Msg {
		ch, err := adapter.StreamChat(ctx, sysPrompt, historySnapshot)
		if err != nil {
			return streamErrMsg{err}
		}
		return waitForToken(ch)()
	}
}

func waitForToken(ch <-chan string) tea.Cmd {
	return func() tea.Msg {
		token, ok := <-ch
		if !ok {
			return streamDoneMsg{}
		}
		return streamTokenMsg{token: token, ch: ch}
	}
}

func (m AgentModel) View() string {
	name := brand.Active().AgentName
	if m.quitting {
		return name + " session terminated.\n"
	}

	var sb strings.Builder
	sb.WriteString("Topological Fault Context Injected:\n")
	sb.WriteString(systemStyle.Render(m.clusterContext) + "\n\n")

	for _, msg := range m.uiHistory {
		sb.WriteString(msg + "\n")
	}

	if m.waiting {
		sb.WriteString("\n" + waitStyle.Render("Streaming response... (press Esc to cancel)"))
	} else {
		sb.WriteString("\n" + promptStyle.Render("Ask "+name+" > ") + m.userInput)
	}

	return sb.String()
}
