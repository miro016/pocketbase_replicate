package cmd

import (
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"

	"github.com/pocketbase/pocketbase/apis"
	"github.com/pocketbase/pocketbase/core"
	"github.com/pocketbase/pocketbase/tools/cluster"
	"github.com/pocketbase/pocketbase/tools/hook"
	"github.com/spf13/cobra"
)

// NewServeCommand creates and returns new command responsible for
// starting the default PocketBase web server.
func NewServeCommand(app core.App, showStartBanner bool) *cobra.Command {
	var allowedOrigins []string
	var httpAddr string
	var httpsAddr string

	// Cluster flags
	var clusterNodeID string
	var clusterSecret string
	var clusterSelfURL string
	var clusterPeers []string

	command := &cobra.Command{
		Use:          "serve [domain(s)]",
		Args:         cobra.ArbitraryArgs,
		Short:        "Starts the web server (default to 127.0.0.1:8090 if no domain is specified)",
		SilenceUsage: true,
		RunE: func(command *cobra.Command, args []string) error {
			// set default listener addresses if at least one domain is specified
			if len(args) > 0 {
				if httpAddr == "" {
					httpAddr = "0.0.0.0:80"
				}
				if httpsAddr == "" {
					httpsAddr = "0.0.0.0:443"
				}
			} else {
				if httpAddr == "" {
					httpAddr = "127.0.0.1:8090"
				}
			}

			// Set up cluster if secret is provided
			if clusterSecret != "" {
				// Warn when HTTPS is not configured: the shared secret travels in
				// plaintext HTTP headers, making it trivially interceptable. (Fix #8)
				if httpsAddr == "" {
					app.Logger().Warn("[cluster] cluster secret is set but HTTPS is not configured; " +
						"cluster traffic (including the shared secret) travels in plaintext. " +
						"Use --https to enable TLS.")
				}

				nodeID := clusterNodeID
				if nodeID == "" {
					// Default node ID: hostname + http address
					hostname, _ := os.Hostname()
					nodeID = fmt.Sprintf("%s-%s", hostname, httpAddr)
				}

				// Normalise peer URLs (strip trailing slashes)
				peers := make([]string, 0, len(clusterPeers))
				for _, p := range clusterPeers {
					p = strings.TrimRight(strings.TrimSpace(p), "/")
					if p != "" {
						peers = append(peers, p)
					}
				}

				manager := cluster.NewManager(nodeID, clusterSecret, clusterSelfURL, peers, app.Logger())

				// Store manager in app store so API handlers can retrieve it
				app.Store().Set(apis.ClusterManagerKey, manager)

				// Wire the apply function (needs access to apis package internals)
				manager.ApplyFunc = func(event *cluster.ReplicationEvent) {
					apis.ApplyReplication(app, event)
				}

				// Start peer connections after the server is ready
				app.OnServe().Bind(&hook.Handler[*core.ServeEvent]{
					Id: "clusterStart",
					Func: func(e *core.ServeEvent) error {
						err := e.Next()
						if err != nil {
							return err
						}
						manager.Start()
						app.Logger().Info("[cluster] node started",
							"nodeId", nodeID, "peers", peers)
						return nil
					},
					Priority: -99,
				})

				// Stop cluster on terminate
				app.OnTerminate().Bind(&hook.Handler[*core.TerminateEvent]{
					Id: "clusterStop",
					Func: func(e *core.TerminateEvent) error {
						manager.Stop()
						return e.Next()
					},
					Priority: 99,
				})
			}

			err := apis.Serve(app, apis.ServeConfig{
				HttpAddr:           httpAddr,
				HttpsAddr:          httpsAddr,
				ShowStartBanner:    showStartBanner,
				AllowedOrigins:     allowedOrigins,
				CertificateDomains: args,
			})

			if errors.Is(err, http.ErrServerClosed) {
				return nil
			}

			return err
		},
	}

	command.PersistentFlags().StringSliceVar(
		&allowedOrigins,
		"origins",
		[]string{"*"},
		"CORS allowed domain origins list",
	)

	command.PersistentFlags().StringVar(
		&httpAddr,
		"http",
		"",
		"TCP address to listen for the HTTP server\n(if domain args are specified - default to 0.0.0.0:80, otherwise - default to 127.0.0.1:8090)",
	)

	command.PersistentFlags().StringVar(
		&httpsAddr,
		"https",
		"",
		"TCP address to listen for the HTTPS server\n(if domain args are specified - default to 0.0.0.0:443, otherwise - default to empty string, aka. no TLS)\nThe incoming HTTP traffic also will be auto redirected to the HTTPS version",
	)

	// Cluster flags
	command.PersistentFlags().StringVar(
		&clusterNodeID,
		"cluster-id",
		"",
		"unique ID for this cluster node (default: hostname+httpAddr)",
	)

	command.PersistentFlags().StringVar(
		&clusterSecret,
		"cluster-secret",
		"",
		"shared secret for cluster peer authentication (enables cluster mode when set)",
	)

	command.PersistentFlags().StringVar(
		&clusterSelfURL,
		"cluster-self-url",
		"",
		"public base URL of this node (e.g. http://node1:8090), sent to peers so they can connect back automatically (enables late-join without pre-configuring all nodes)",
	)

	command.PersistentFlags().StringSliceVar(
		&clusterPeers,
		"cluster-peers",
		[]string{},
		"comma-separated list of peer node base URLs (e.g. http://node2:8091,http://node3:8092)",
	)

	return command
}
