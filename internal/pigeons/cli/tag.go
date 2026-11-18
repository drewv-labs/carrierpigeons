package cli

import (
	"context"
	"fmt"
	"time"

	"github.com/spf13/cobra"
)

var removeTagFlag bool

var tagCmd = &cobra.Command{
	Use:   "tag <tag_name> <node_id...>",
	Short: "Attach (or --remove) a hub-authoritative tag on one or more nodes",
	Example: `  pigeons tag databroker rv64-coop-01
  pigeons tag compute-bench rv64-compute-01 rv64-compute-02
  pigeons tag compute-bench rv64-compute-02 --remove`,
	Args: cobra.MinimumNArgs(2),
	RunE: func(_ *cobra.Command, args []string) error {
		tags := cleanSelectorTokens([]string{args[0]}, true)
		if len(tags) == 0 {
			return fmt.Errorf("invalid empty tag %q", args[0])
		}
		nodes := cleanSelectorTokens(args[1:], false)

		ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
		defer cancel()

		store, err := openOperatorStore(ctx)
		if err != nil {
			return fmt.Errorf("connect CTD Ledger: %w", err)
		}
		defer store.Close()

		for _, nodeID := range nodes {
			if removeTagFlag {
				merged, err := store.RemoveNodeTags(ctx, nodeID, tags)
				if err != nil {
					return err
				}
				fmt.Printf("[-] Removed %v from %s -> effective tags: %v\n", tags, nodeID, merged)
			} else {
				merged, err := store.AddNodeTags(ctx, nodeID, tags)
				if err != nil {
					return err
				}
				fmt.Printf("[+] Tagged %s with %v -> effective tags: %v\n", nodeID, tags, merged)
			}
		}
		return nil
	},
}

func init() {
	rootCmd.AddCommand(tagCmd)
	tagCmd.Flags().BoolVarP(&removeTagFlag, "remove", "r", false, "Remove the specified tag from the target node(s)")
}
