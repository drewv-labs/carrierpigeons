package cli

import (
	"context"
	"fmt"
	"time"

	"github.com/spf13/cobra"
)

var tagCmd = &cobra.Command{
	Use:   "tag <tag_name> <node_id...>",
	Short: "Attach (or --remove) a hub-authoritative tag on one or more nodes",
	Example: `  pigeons tag databroker rv64-coop-01
  pigeons tag compute-bench rv64-compute-01 rv64-compute-02
  pigeons tag compute-bench rv64-compute-02 --remove`,
	Args: cobra.MinimumNArgs(2),
	RunE: func(cmd *cobra.Command, args []string) error {
		tags := cleanSelectorTokens([]string{args[0]}, true)
		if len(tags) == 0 {
			return fmt.Errorf("invalid empty tag %q", args[0])
		}
		nodes := cleanSelectorTokens(args[1:], false)

		parentCtx := cmd.Context()
		if parentCtx == nil {
			parentCtx = context.Background()
		}
		ctx, cancel := context.WithTimeout(parentCtx, 8*time.Second)
		defer cancel()

		state := cliStateFromCommand(cmd)
		out := cmd.OutOrStdout()
		removeTag, _ := cmd.Flags().GetBool("remove")

		store, err := openOperatorStoreForConfig(ctx, state.wsCfg)
		if err != nil {
			return fmt.Errorf("connect CTD Ledger: %w", err)
		}
		defer store.Close()

		for _, nodeID := range nodes {
			if removeTag {
				merged, err := store.RemoveNodeTags(ctx, nodeID, tags)
				if err != nil {
					return err
				}
				fmt.Fprintf(out, "[-] Removed %v from %s -> effective tags: %v\n", tags, nodeID, merged)
			} else {
				merged, err := store.AddNodeTags(ctx, nodeID, tags)
				if err != nil {
					return err
				}
				fmt.Fprintf(out, "[+] Tagged %s with %v -> effective tags: %v\n", nodeID, tags, merged)
			}
		}
		return nil
	},
}

func init() {
	rootCmd.AddCommand(tagCmd)
	tagCmd.Flags().BoolP("remove", "r", false, "Remove the specified tag from the target node(s)")
}
