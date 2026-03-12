// Package cluster implements multi-node replication for PocketBase.
//
// Architecture overview:
//   - Each node exposes GET /api/cluster/events (SSE stream, authenticated via shared secret)
//   - Each node also acts as an SSE client connecting to configured peer URLs
//   - When a model changes, the originating node broadcasts a replication event to peers
//   - Peers apply the change locally (DB write + SSE broadcast to their own clients)
//   - Loop prevention: a sync.Map tracks currently-replicating operations so that
//     applying a peer's event doesn't cause it to be re-broadcast back
package cluster

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// OpCreate, OpUpdate, OpDelete are the replication operation types.
const (
	OpCreate = "create"
	OpUpdate = "update"
	OpDelete = "delete"
)

// errSelfConnect is returned by connectToPeer when the remote endpoint
// reports that we tried to connect to ourselves. The goroutine should stop
// retrying and remove the entry from peerConns.
var errSelfConnect = errors.New("cannot connect to self")

// ReplicationEvent represents a single replicated database operation.
type ReplicationEvent struct {
	Op        string         `json:"op"`               // create | update | delete
	Table     string         `json:"table"`            // table/collection name
	ID        string         `json:"id"`               // record primary key
	RawData   map[string]any `json:"data"`             // serialized row (nil for delete)
	Origin    string         `json:"origin"`           // node ID that originated the change
	Seq       int64          `json:"seq"`              // monotonically increasing sequence
	OriginURL string         `json:"originUrl,omitempty"` // public base URL of originating node (for file replication)
}

// NodeInfo holds metadata about a connected peer node, used by the admin UI.
type NodeInfo struct {
	ID          string    `json:"id"`
	Addr        string    `json:"addr"`
	ConnectedAt time.Time `json:"connectedAt"`
	Status      string    `json:"status"` // "connected" | "reconnecting"
}

// -----------------------------------------------------------------
// Loop-prevention registry
// -----------------------------------------------------------------

// replicatingNow is a set of "table:id" keys for operations that are
// currently being applied from a peer (so our own hooks should not
// re-broadcast them back to peers).
var replicatingNow sync.Map

// ReplicationKey builds the de-duplication key for a table/id pair.
func ReplicationKey(table, id string) string {
	return table + ":" + id
}

// MarkReplicating registers a table:id as currently being applied from a peer.
func MarkReplicating(key string) {
	replicatingNow.Store(key, struct{}{})
}

// UnmarkReplicating removes the table:id from the replication registry.
func UnmarkReplicating(key string) {
	replicatingNow.Delete(key)
}

// IsReplicating reports whether the given key is currently being applied from a peer.
func IsReplicating(key string) bool {
	_, ok := replicatingNow.Load(key)
	return ok
}

// -----------------------------------------------------------------
// Manager
// -----------------------------------------------------------------

// Manager is the central component that manages:
//   - the list of SSE client connections from peer nodes
//   - outgoing goroutines that subscribe to each peer's SSE stream
//   - broadcasting local model changes to all connected peers
type Manager struct {
	nodeID   string
	secret   string
	selfURL  string // public base URL of this node, sent to peers for auto reverse-connect
	peerURLs []string
	logger   *slog.Logger

	seq atomic.Int64

	// sseClients holds channels for each peer that is connected TO US (reading our events).
	// key: nodeID of the peer
	mu         sync.RWMutex
	sseClients map[string]*sseClient // our SSE subscribers (peers reading from us)
	peerConns  map[string]*peerConn  // our outgoing connections (us reading from peers)

	// stopCh is closed when the manager shuts down.
	stopCh chan struct{}

	// ApplyFunc is called by the manager when a replication event arrives from a peer.
	// It is wired up by apis/cluster.go after bootstrapping.
	ApplyFunc func(event *ReplicationEvent)

	// LoadPeerSyncTime, if set, returns the last successful sync time for a peer URL.
	// Used to send X-Cluster-Since on reconnect, enabling delta sync instead of full sync.
	// When nil, the in-memory connectedAt is used (lost on restart → full sync).
	LoadPeerSyncTime func(peerURL string) time.Time

	// SavePeerSyncTime, if set, persists the sync time after a peer connection is established.
	SavePeerSyncTime func(peerURL string, syncedAt time.Time)
}

// sseClient represents a peer node that is subscribed to our SSE event stream.
type sseClient struct {
	nodeID      string
	addr        string
	selfURL     string // public base URL advertised by the peer (for gossip)
	connectedAt time.Time
	ch          chan []byte    // lines to write as SSE events
	done        chan struct{}  // closed to signal the SSE handler to stop (e.g. buffer overflow)
	closeOnce   sync.Once
}

// forceClose signals the SSE handler to close this connection. Idempotent.
func (c *sseClient) forceClose() {
	c.closeOnce.Do(func() {
		close(c.done)
	})
}

type peerConn struct {
	nodeID      string
	addr        string
	connectedAt time.Time
	status      string
	mu          sync.Mutex
}

// NewManager creates a new cluster manager.
//
// nodeID is this node's unique identifier (e.g. "node1").
// secret is the shared secret used to authenticate peer connections.
// selfURL is the public base URL of this node (e.g. "http://node1:8090"), sent to peers
// so they can establish a reverse connection. Leave empty to disable auto-reverse-connect.
// peerURLs is a list of base HTTP URLs for peer nodes (e.g. ["http://node2:8091"]).
func NewManager(nodeID, secret, selfURL string, peerURLs []string, logger *slog.Logger) *Manager {
	if logger == nil {
		logger = slog.Default()
	}
	return &Manager{
		nodeID:     nodeID,
		secret:     secret,
		selfURL:    strings.TrimRight(selfURL, "/"),
		peerURLs:   peerURLs,
		logger:     logger,
		sseClients: make(map[string]*sseClient),
		peerConns:  make(map[string]*peerConn),
		stopCh:     make(chan struct{}),
	}
}

// isValidPeerURL reports whether peerURL is an acceptable peer address.
// Only http and https schemes are allowed to prevent SSRF via gossip injection.
func isValidPeerURL(peerURL string) bool {
	u, err := url.Parse(peerURL)
	return err == nil && (u.Scheme == "http" || u.Scheme == "https") && u.Host != ""
}

// AddPeer dynamically adds a new peer URL and starts maintaining a connection to it.
// If the URL is already tracked, matches this node's own selfURL, or is not a valid
// http/https URL, this is a no-op.
func (m *Manager) AddPeer(peerURL string) {
	peerURL = strings.TrimRight(peerURL, "/")
	if peerURL == "" {
		return
	}
	if !isValidPeerURL(peerURL) {
		m.logger.Warn("[cluster] ignoring peer URL with invalid scheme", "url", peerURL)
		return
	}
	// Never connect to ourselves.
	if m.selfURL != "" && peerURL == m.selfURL {
		return
	}

	m.mu.Lock()
	if _, exists := m.peerConns[peerURL]; exists {
		m.mu.Unlock()
		return
	}
	// Pre-register so concurrent AddPeer calls for the same URL are no-ops.
	m.peerConns[peerURL] = &peerConn{addr: peerURL, status: "reconnecting"}
	m.mu.Unlock()

	m.logger.Info("[cluster] adding dynamic peer", "url", peerURL)
	go m.maintainPeerConnection(peerURL)
}

// NodeID returns the ID of this node.
func (m *Manager) NodeID() string {
	return m.nodeID
}

// Secret returns the cluster secret.
func (m *Manager) Secret() string {
	return m.secret
}

// SelfURL returns the public base URL of this node (may be empty if not configured).
func (m *Manager) SelfURL() string {
	return m.selfURL
}

// Start launches goroutines that connect to each configured peer.
// It is non-blocking and returns immediately.
func (m *Manager) Start() {
	for _, peerURL := range m.peerURLs {
		url := peerURL
		go m.maintainPeerConnection(url)
	}
}

// Stop shuts down the manager and all active connections.
func (m *Manager) Stop() {
	select {
	case <-m.stopCh:
		// already stopped
	default:
		close(m.stopCh)
	}
}

// Done returns a channel that is closed when the manager stops.
// External goroutines (e.g. pruneClusterLog) can select on this to exit cleanly.
func (m *Manager) Done() <-chan struct{} {
	return m.stopCh
}

// Broadcast sends a replication event to all currently-connected SSE peer clients.
// If a client's buffer is full the connection is force-closed, causing the peer to
// reconnect and perform a delta sync to recover the missed events.
func (m *Manager) Broadcast(event *ReplicationEvent) {
	event.Origin = m.nodeID
	event.Seq = m.seq.Add(1)
	if event.OriginURL == "" {
		event.OriginURL = m.selfURL
	}

	data, err := json.Marshal(event)
	if err != nil {
		m.logger.Warn("[cluster] failed to marshal replication event", "error", err)
		return
	}

	line := []byte("event: replicate\ndata: " + string(data) + "\n\n")

	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, c := range m.sseClients {
		select {
		case c.ch <- line:
		default:
			// Buffer full: force-close the connection so the peer reconnects and
			// performs a delta sync to recover the dropped events.
			m.logger.Warn("[cluster] SSE client buffer full, forcing reconnect for delta sync",
				"peer", c.nodeID)
			c.forceClose()
		}
	}
}

// BroadcastToOne sends a replication event to a single connected peer by node ID.
// If the client's buffer is full the connection is force-closed (same as Broadcast).
func (m *Manager) BroadcastToOne(nodeID string, event *ReplicationEvent) {
	event.Origin = m.nodeID
	event.Seq = m.seq.Add(1)
	if event.OriginURL == "" {
		event.OriginURL = m.selfURL
	}

	data, err := json.Marshal(event)
	if err != nil {
		m.logger.Warn("[cluster] failed to marshal replication event", "error", err)
		return
	}

	line := []byte("event: replicate\ndata: " + string(data) + "\n\n")

	m.mu.RLock()
	c, ok := m.sseClients[nodeID]
	m.mu.RUnlock()

	if !ok {
		return
	}

	select {
	case c.ch <- line:
	default:
		m.logger.Warn("[cluster] SSE client buffer full, forcing reconnect", "peer", nodeID)
		c.forceClose()
	}
}

// SendToPeer sends a replication event to a single peer, blocking until the peer's
// channel has space or ctx is cancelled. Returns false if the peer is gone or ctx expired.
// Used for initial sync where dropping events would silently corrupt the peer's state.
func (m *Manager) SendToPeer(nodeID string, event *ReplicationEvent, ctx context.Context) bool {
	event.Origin = m.nodeID
	event.Seq = m.seq.Add(1)
	if event.OriginURL == "" {
		event.OriginURL = m.selfURL
	}

	data, err := json.Marshal(event)
	if err != nil {
		m.logger.Warn("[cluster] failed to marshal replication event", "error", err)
		return false
	}

	line := []byte("event: replicate\ndata: " + string(data) + "\n\n")

	m.mu.RLock()
	c, ok := m.sseClients[nodeID]
	m.mu.RUnlock()

	if !ok {
		return false
	}

	select {
	case c.ch <- line:
		return true
	case <-ctx.Done():
		return false
	case <-c.done:
		return false
	}
}

// RegisterSSEClient registers a new peer that has connected to our SSE endpoint.
// selfURL is the peer's own public base URL (from X-Cluster-Self-URL header), used for gossip.
//
// Returns:
//   - eventCh: channel of SSE payload bytes to write to the HTTP response
//   - doneCh: closed when the manager wants to force-close this connection (e.g. buffer overflow)
//   - cleanup: must be deferred by the caller; removes the client from the registry
func (m *Manager) RegisterSSEClient(nodeID, addr, selfURL string) (<-chan []byte, <-chan struct{}, func()) {
	c := &sseClient{
		nodeID:      nodeID,
		addr:        addr,
		selfURL:     strings.TrimRight(selfURL, "/"),
		connectedAt: time.Now().UTC(),
		ch:          make(chan []byte, 4096),
		done:        make(chan struct{}),
	}

	m.mu.Lock()
	m.sseClients[nodeID] = c
	m.mu.Unlock()

	m.logger.Info("[cluster] peer connected to our SSE stream", "peer", nodeID, "addr", addr)

	cleanup := func() {
		m.mu.Lock()
		// Only delete if the map still points to this exact client instance.
		// A fast reconnect may have replaced this entry with a new client; in that
		// case we must not remove the new connection. (Fix #3)
		if current, ok := m.sseClients[nodeID]; ok && current == c {
			delete(m.sseClients, nodeID)
		}
		m.mu.Unlock()
		c.forceClose() // idempotent; signals doneCh if not already closed
		m.logger.Info("[cluster] peer disconnected from our SSE stream", "peer", nodeID)
	}

	return c.ch, c.done, cleanup
}

// Nodes returns a snapshot of all known peer nodes (both inbound SSE clients and outgoing connections).
func (m *Manager) Nodes() []NodeInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()

	seen := make(map[string]struct{})
	var nodes []NodeInfo

	for id, c := range m.sseClients {
		seen[id] = struct{}{}
		nodes = append(nodes, NodeInfo{
			ID:          c.nodeID,
			Addr:        c.addr,
			ConnectedAt: c.connectedAt,
			Status:      "connected",
		})
	}

	for peerURL, pc := range m.peerConns {
		pc.mu.Lock()
		nodeID := pc.nodeID
		info := NodeInfo{
			ID:          nodeID,
			Addr:        peerURL,
			ConnectedAt: pc.connectedAt,
			Status:      pc.status,
		}
		pc.mu.Unlock()
		// Only deduplicate by nodeID when the peer is identified (connected).
		// Reconnecting peers (nodeID=="") always appear individually by URL so
		// all configured peers are visible in the admin UI. (Fix #4)
		if nodeID != "" {
			if _, ok := seen[nodeID]; ok {
				continue
			}
			seen[nodeID] = struct{}{}
		}
		nodes = append(nodes, info)
	}

	return nodes
}

// KnownPeerURLs returns the public base URLs of all currently connected peers
// (both inbound SSE clients that advertised a selfURL and outgoing peerConns).
// excludeURL is filtered out (typically the requesting peer's own URL).
func (m *Manager) KnownPeerURLs(excludeURL string) []string {
	excludeURL = strings.TrimRight(excludeURL, "/")

	m.mu.RLock()
	defer m.mu.RUnlock()

	seen := make(map[string]struct{})
	var urls []string

	for _, c := range m.sseClients {
		if c.selfURL != "" && c.selfURL != excludeURL {
			if _, ok := seen[c.selfURL]; !ok {
				seen[c.selfURL] = struct{}{}
				urls = append(urls, c.selfURL)
			}
		}
	}

	for url := range m.peerConns {
		if url != excludeURL {
			if _, ok := seen[url]; !ok {
				seen[url] = struct{}{}
				urls = append(urls, url)
			}
		}
	}

	return urls
}

// -----------------------------------------------------------------
// Outgoing peer SSE connection (we read from peers)
// -----------------------------------------------------------------

// maintainPeerConnection maintains a persistent SSE connection to a peer.
// It reconnects with exponential back-off when the connection drops.
// If a permanent error (e.g. self-connection) is detected, it removes the
// entry from peerConns and exits without retrying.
func (m *Manager) maintainPeerConnection(peerBaseURL string) {
	backoff := 2 * time.Second
	maxBackoff := 60 * time.Second

	// Reuse the peerConn placeholder that AddPeer may have pre-registered,
	// or create a new one if this goroutine was started from Start().
	m.mu.Lock()
	pc, exists := m.peerConns[peerBaseURL]
	if !exists {
		pc = &peerConn{addr: peerBaseURL, status: "reconnecting"}
		m.peerConns[peerBaseURL] = pc
	}
	m.mu.Unlock()

	for {
		select {
		case <-m.stopCh:
			return
		default:
		}

		err := m.connectToPeer(peerBaseURL, pc)
		if err != nil {
			// Permanent errors must not be retried.
			if errors.Is(err, errSelfConnect) {
				m.mu.Lock()
				delete(m.peerConns, peerBaseURL)
				m.mu.Unlock()
				m.logger.Debug("[cluster] skipping self-connection", "url", peerBaseURL)
				return
			}

			pc.mu.Lock()
			pc.status = "reconnecting"
			pc.mu.Unlock()

			m.logger.Warn("[cluster] peer connection failed, retrying",
				"peer", peerBaseURL, "error", err, "backoff", backoff)

			// Add jitter (±25%) to prevent thundering-herd reconnection storms.
			jitter := time.Duration(float64(backoff) * (0.75 + rand.Float64()*0.5))
			select {
			case <-m.stopCh:
				return
			case <-time.After(jitter):
			}

			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
		} else {
			backoff = 2 * time.Second
		}
	}
}

// connectToPeer establishes one SSE connection to a peer and reads events until it closes.
func (m *Manager) connectToPeer(peerBaseURL string, pc *peerConn) error {
	eventsURL := strings.TrimRight(peerBaseURL, "/") + "/api/cluster/events"

	// Use a context tied to stopCh so that both the TCP connect phase and the
	// streaming phase are cancelled immediately when the manager stops. A 15-second
	// dial timeout is also applied so that a firewall-dropped packet does not block
	// the goroutine for the OS TCP timeout (~2 min on Linux).
	connCtx, connCancel := context.WithCancel(context.Background())
	defer connCancel()
	go func() {
		select {
		case <-m.stopCh:
			connCancel()
		case <-connCtx.Done():
		}
	}()

	req, err := http.NewRequestWithContext(connCtx, "GET", eventsURL, nil)
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}

	// Determine the last sync time for this peer.
	// LoadPeerSyncTime (if wired) reads from persistent storage so the timestamp
	// survives process restarts, enabling delta sync instead of full sync. (Fix #6)
	var lastConnectedAt time.Time
	if m.LoadPeerSyncTime != nil {
		lastConnectedAt = m.LoadPeerSyncTime(peerBaseURL)
	}
	if lastConnectedAt.IsZero() {
		// Fall back to in-memory connectedAt (available if the process has not restarted).
		pc.mu.Lock()
		lastConnectedAt = pc.connectedAt
		pc.mu.Unlock()
	}

	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set("Cache-Control", "no-cache")
	req.Header.Set("X-Cluster-Node-ID", m.nodeID)
	req.Header.Set("X-Cluster-Secret", m.secret)
	if m.selfURL != "" {
		req.Header.Set("X-Cluster-Self-URL", m.selfURL)
	}
	if !lastConnectedAt.IsZero() {
		req.Header.Set("X-Cluster-Since", lastConnectedAt.UTC().Format(time.RFC3339Nano))
	}

	client := &http.Client{
		Timeout: 0, // no overall timeout for SSE; dial timeout is set on the transport
		Transport: &http.Transport{
			DialContext: (&net.Dialer{
				Timeout:   15 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext,
		},
	}

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("connect to peer: %w", err)
	}
	defer resp.Body.Close()

	// Detect self-connection: the server always echoes its node ID in the response header.
	if resp.Header.Get("X-Cluster-Node-ID") == m.nodeID {
		return errSelfConnect
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("peer returned status %d", resp.StatusCode)
	}

	// Read the peer nodeID from the hello event
	peerNodeID := resp.Header.Get("X-Cluster-Node-ID")
	if peerNodeID == "" {
		peerNodeID = peerBaseURL
	}

	now := time.Now().UTC()
	pc.mu.Lock()
	pc.nodeID = peerNodeID
	pc.connectedAt = now
	pc.status = "connected"
	pc.mu.Unlock()

	// Persist the sync time so that future reconnects (even after restart) use delta sync. (Fix #6)
	if m.SavePeerSyncTime != nil {
		m.SavePeerSyncTime(peerBaseURL, now)
	}

	m.logger.Info("[cluster] connected to peer SSE stream", "peer", peerNodeID, "url", eventsURL)

	m.mu.Lock()
	m.peerConns[peerBaseURL] = pc
	m.mu.Unlock()

	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 64*1024), 8*1024*1024) // start 64KB, max 8MB per SSE event

	var eventType string
	var dataBuf bytes.Buffer

	for scanner.Scan() {
		select {
		case <-m.stopCh:
			return nil
		default:
		}

		line := scanner.Text()

		switch {
		case strings.HasPrefix(line, "event:"):
			eventType = strings.TrimSpace(strings.TrimPrefix(line, "event:"))

		case strings.HasPrefix(line, "data:"):
			data := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
			dataBuf.WriteString(data)

		case line == "":
			// End of event
			switch {
			case eventType == "replicate" && dataBuf.Len() > 0:
				m.handlePeerEvent(peerBaseURL, dataBuf.Bytes())
			case eventType == "peers" && dataBuf.Len() > 0:
				m.handlePeersEvent(dataBuf.Bytes())
			}
			eventType = ""
			dataBuf.Reset()
		}
	}

	if err := scanner.Err(); err != nil && err != io.EOF {
		return fmt.Errorf("reading peer stream: %w", err)
	}

	return nil
}

// handlePeersEvent receives a gossip peers list and connects to any unknown nodes.
// URLs are validated before connecting to prevent SSRF via gossip injection. (Fix #13)
func (m *Manager) handlePeersEvent(data []byte) {
	var payload struct {
		URLs []string `json:"urls"`
	}
	if err := json.Unmarshal(data, &payload); err != nil {
		m.logger.Warn("[cluster] failed to decode peers gossip event", "error", err)
		return
	}
	for _, u := range payload.URLs {
		if !isValidPeerURL(u) {
			m.logger.Warn("[cluster] ignoring invalid peer URL from gossip", "url", u)
			continue
		}
		m.AddPeer(u) // idempotent: no-op if already connected
	}
}

// handlePeerEvent decodes and applies a replication event received from a peer.
// peerBaseURL is the base URL of the peer we received this event from; it is used
// as a fallback OriginURL for file replication when the event doesn't carry one
// (e.g. from older nodes that don't set OriginURL).
func (m *Manager) handlePeerEvent(peerBaseURL string, data []byte) {
	var event ReplicationEvent
	if err := json.Unmarshal(data, &event); err != nil {
		m.logger.Warn("[cluster] failed to decode peer event", "error", err)
		return
	}

	// Skip events that originated from this node (shouldn't happen with proper loop prevention,
	// but acts as a safety net).
	if event.Origin == m.nodeID {
		return
	}

	// If the event doesn't carry an origin URL, fall back to the peer we received it from.
	// This works correctly for direct 2-node connections; for relayed events the originating
	// node must set OriginURL explicitly (which Broadcast/SendToPeer now does via selfURL).
	if event.OriginURL == "" {
		event.OriginURL = peerBaseURL
	}

	m.logger.Debug("[cluster] received replication event",
		"op", event.Op, "table", event.Table, "id", event.ID, "origin", event.Origin)

	if m.ApplyFunc != nil {
		m.ApplyFunc(&event)
	}
}
