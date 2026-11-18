package controlplane

import (
	"github.com/drewv-labs/carrier-pigeons/internal/pigeons/cli"
	"github.com/drewv-labs/carrier-pigeons/pkg/brand"
	"github.com/spf13/cobra"
)

// RootCommand returns the Pigeons CLI/TUI command tree customized with a whitelabel BrandSpec.
func RootCommand(spec brand.Spec) *cobra.Command {
	cli.ApplyBrand(spec)
	return cli.RootCommand()
}

// ExecuteWithBrand runs the Pigeons CLI/TUI workstation control plane under a downstream BrandSpec.
func ExecuteWithBrand(spec brand.Spec) {
	cli.ApplyBrand(spec)
	cli.Execute()
}
