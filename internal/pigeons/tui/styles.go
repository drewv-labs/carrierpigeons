package tui

import (
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

var (
	userStyle   = lipgloss.NewStyle().Foreground(lipgloss.Color("86")).Bold(true)
	agentStyle  = lipgloss.NewStyle().Foreground(lipgloss.Color("205"))
	systemStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("214")).Italic(true)
	waitStyle   = lipgloss.NewStyle().Foreground(lipgloss.Color("241")).Italic(true)
)

type streamTokenMsg struct {
	token string
	ch    <-chan string
}

type streamDoneMsg struct{}

type streamErrMsg struct {
	err error
}

func waitForToken(ch <-chan string) tea.Cmd {
	return func() tea.Msg {
		tok, ok := <-ch
		if !ok {
			return streamDoneMsg{}
		}
		return streamTokenMsg{token: tok, ch: ch}
	}
}
