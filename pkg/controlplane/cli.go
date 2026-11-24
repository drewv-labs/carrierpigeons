package controlplane

import (
	"github.com/drewv-labs/carrierpigeons/internal/pigeons/cli"
	"github.com/drewv-labs/carrierpigeons/pkg/brand"
	"github.com/spf13/cobra"
)

// ControlPlane holds an instance-scoped BrandSpec for embedding the Pigeons CLI/TUI control plane.
type ControlPlane struct {
	brand brand.Spec
}

// New constructs an instance-scoped ControlPlane configured with spec without mutating global brand state.
func New(spec brand.Spec) *ControlPlane {
	return &ControlPlane{brand: spec.MergeWithDefaults()}
}

// Brand returns the ControlPlane instance's effective BrandSpec.
func (c *ControlPlane) Brand() brand.Spec {
	return c.brand
}

// RootCommand returns an isolated Pigeons CLI/TUI command tree customized with this ControlPlane's BrandSpec.
func (c *ControlPlane) RootCommand() *cobra.Command {
	return cli.NewRootCommand(c.brand)
}

// Execute runs the Pigeons CLI/TUI workstation control plane with this ControlPlane's BrandSpec.
func (c *ControlPlane) Execute() {
	cli.ExecuteWithBrand(c.brand)
}

// RootCommand returns the Pigeons CLI/TUI command tree customized with a whitelabel BrandSpec.
func RootCommand(spec brand.Spec) *cobra.Command {
	return New(spec).RootCommand()
}

// ExecuteWithBrand runs the Pigeons CLI/TUI workstation control plane under a downstream BrandSpec.
func ExecuteWithBrand(spec brand.Spec) {
	New(spec).Execute()
}
