<script>
    import { onDestroy } from "svelte";
    import ApiClient from "@/utils/ApiClient";
    import { pageTitle } from "@/stores/app";
    import PageWrapper from "@/components/base/PageWrapper.svelte";
    import RefreshButton from "@/components/base/RefreshButton.svelte";

    $pageTitle = "Cluster";

    let isLoading = false;
    let clusterInfo = { nodeId: "", enabled: false, nodes: [] };
    let refreshInterval = null;

    loadClusterInfo();

    // Auto-refresh every 5 seconds
    refreshInterval = setInterval(loadClusterInfo, 5000);

    onDestroy(() => {
        clearInterval(refreshInterval);
    });

    async function loadClusterInfo() {
        isLoading = true;
        try {
            const response = await ApiClient.send("/api/cluster/nodes", { method: "GET" });
            clusterInfo = response || { nodeId: "", enabled: false, nodes: [] };
        } catch (err) {
            if (!err?.isAbort) {
                console.warn("Failed to load cluster info:", err);
            }
        }
        isLoading = false;
    }

    function formatDate(dateStr) {
        if (!dateStr) return "—";
        try {
            const d = new Date(dateStr);
            return d.toLocaleString();
        } catch (_) {
            return dateStr;
        }
    }

    function statusClass(status) {
        if (status === "connected") return "badge-success";
        if (status === "reconnecting") return "badge-warning";
        return "badge-danger";
    }

    function formatClockSkew(ms) {
        if (ms === undefined || ms === null || ms === 0) return "—";
        const abs = Math.abs(ms);
        const sign = ms > 0 ? "+" : "-";
        if (abs < 1000) return sign + abs + "ms";
        return sign + (abs / 1000).toFixed(1) + "s";
    }

    function clockSkewClass(ms) {
        if (ms === undefined || ms === null) return "";
        const abs = Math.abs(ms);
        if (abs > 5000) return "txt-danger";
        if (abs > 1000) return "txt-warning";
        return "";
    }

    // Check if any peer has significant clock skew (>1s)
    $: hasClockSkewWarning = clusterInfo.nodes?.some(
        (n) => n.clockSkewMs && Math.abs(n.clockSkewMs) > 1000
    );
</script>

<PageWrapper>
    <header class="page-header">
        <nav class="breadcrumbs">
            <div class="breadcrumb-item">Cluster</div>
        </nav>
        <RefreshButton on:refresh={loadClusterInfo} />
    </header>

    {#if !clusterInfo.enabled}
        <div class="placeholder-section m-b-base">
            <div class="icon">
                <i class="ri-node-tree" />
            </div>
            <h5 class="m-t-sm m-b-xs">Cluster mode is disabled</h5>
            <p class="txt-hint txt-center m-b-xs">
                Start PocketBase with <code>--cluster-secret</code> and
                <code>--cluster-peers</code> flags to enable multi-node replication.
            </p>
            <div class="content m-t-base">
                <pre class="code-block"><code>./pocketbase serve \
  --cluster-secret=my-shared-secret \
  --cluster-peers=http://node2:8091</code></pre>
            </div>
        </div>
    {:else}
        <!-- Current node info -->
        <div class="section-title">
            <span>
                This node
                <small class="label label-success m-l-5">online</small>
            </span>
        </div>

        <div class="card m-b-base">
            <div class="card-body">
                <div class="field-group">
                    <div class="field">
                        <label class="field-label">Node ID</label>
                        <div class="field-value">
                            <span class="label">{clusterInfo.nodeId || "—"}</span>
                        </div>
                    </div>
                </div>
            </div>
        </div>

        <!-- Clock skew warning -->
        {#if hasClockSkewWarning}
            <div class="alert alert-warning m-b-base">
                <div class="icon">
                    <i class="ri-alarm-warning-line" />
                </div>
                <div class="content">
                    <p>
                        <strong>Clock skew detected!</strong> One or more peers have a time difference
                        greater than 1 second. This can cause incorrect conflict resolution (last-write-wins
                        depends on synchronized clocks). Consider using NTP to synchronize node clocks.
                    </p>
                </div>
            </div>
        {/if}

        <!-- Connected peers -->
        <div class="section-title m-t-base">
            <span>
                Connected peers
                <small class="txt-hint">({clusterInfo.nodes?.length || 0})</small>
            </span>
        </div>

        {#if isLoading && (!clusterInfo.nodes || clusterInfo.nodes.length === 0)}
            <div class="loader" />
        {:else if !clusterInfo.nodes || clusterInfo.nodes.length === 0}
            <div class="placeholder-section">
                <div class="icon">
                    <i class="ri-wifi-off-line" />
                </div>
                <h5 class="m-t-sm m-b-xs">No peers connected</h5>
                <p class="txt-hint txt-center">
                    Make sure the peer nodes are running and configured with the same
                    <code>--cluster-secret</code>.
                </p>
            </div>
        {:else}
            <div class="table-wrapper">
                <table class="table">
                    <thead>
                        <tr>
                            <th class="col-type-text col-field-nodeId">Node ID</th>
                            <th class="col-type-text col-field-addr">Address</th>
                            <th class="col-type-text col-field-status">Status</th>
                            <th class="col-type-text col-field-clockSkew">Clock Skew</th>
                            <th class="col-type-text col-field-lastAck">Last Ack Seq</th>
                            <th class="col-type-date col-field-connectedAt">Connected since</th>
                        </tr>
                    </thead>
                    <tbody>
                        {#each clusterInfo.nodes as node (node.id)}
                            <tr>
                                <td class="col-type-text col-field-nodeId">
                                    <span class="txt-mono">{node.id || "—"}</span>
                                </td>
                                <td class="col-type-text col-field-addr">
                                    <span class="txt-hint">{node.addr || "—"}</span>
                                </td>
                                <td class="col-type-text col-field-status">
                                    <span class="badge {statusClass(node.status)}">
                                        {node.status || "unknown"}
                                    </span>
                                </td>
                                <td class="col-type-text col-field-clockSkew">
                                    <span class="{clockSkewClass(node.clockSkewMs)}">
                                        {formatClockSkew(node.clockSkewMs)}
                                    </span>
                                </td>
                                <td class="col-type-text col-field-lastAck">
                                    <span class="txt-mono">{node.lastAckedSeq || "—"}</span>
                                </td>
                                <td class="col-type-date col-field-connectedAt">
                                    {formatDate(node.connectedAt)}
                                </td>
                            </tr>
                        {/each}
                    </tbody>
                </table>
            </div>
        {/if}

        <!-- Replication info box -->
        <div class="alert alert-info m-t-base">
            <div class="icon">
                <i class="ri-information-line" />
            </div>
            <div class="content">
                <p>
                    All database changes are automatically replicated to connected peers via
                    Server-Sent Events. Clients subscribed to realtime on any node will receive
                    updates triggered by changes on any other node.
                </p>
            </div>
        </div>
    {/if}
</PageWrapper>

<style>
    .code-block {
        text-align: left;
        display: inline-block;
        background: var(--baseAlt1Color);
        border-radius: var(--baseRadius);
        padding: 10px 15px;
        white-space: pre;
    }
    .badge-success {
        background: var(--successColor);
        color: #fff;
    }
    .badge-warning {
        background: var(--warningColor);
        color: #fff;
    }
    .badge-danger {
        background: var(--dangerColor);
        color: #fff;
    }
    .badge {
        padding: 2px 8px;
        border-radius: 10px;
        font-size: 0.8em;
        font-weight: 600;
    }
    .txt-mono {
        font-family: monospace;
    }
    .label-success {
        background: var(--successColor);
        color: #fff;
        padding: 1px 6px;
        border-radius: 10px;
        font-size: 0.75em;
    }
    .card {
        background: var(--baseColor);
        border: 1px solid var(--baseAlt2Color);
        border-radius: var(--baseRadius);
    }
    .card-body {
        padding: 15px;
    }
    .field-group {
        display: flex;
        gap: 15px;
    }
    .field-label {
        font-size: 0.8em;
        color: var(--txtHintColor);
        margin-bottom: 4px;
    }
    .alert {
        display: flex;
        gap: 10px;
        padding: 12px 15px;
        border-radius: var(--baseRadius);
        border: 1px solid;
    }
    .alert-info {
        background: color-mix(in srgb, var(--infoColor) 10%, transparent);
        border-color: color-mix(in srgb, var(--infoColor) 30%, transparent);
    }
    .alert-warning {
        background: color-mix(in srgb, var(--warningColor) 10%, transparent);
        border-color: color-mix(in srgb, var(--warningColor) 30%, transparent);
    }
    .alert-warning .icon {
        color: var(--warningColor);
    }
    .txt-danger {
        color: var(--dangerColor);
        font-weight: 600;
    }
    .txt-warning {
        color: var(--warningColor);
        font-weight: 600;
    }
    .alert .icon {
        font-size: 1.2em;
        color: var(--infoColor);
        flex-shrink: 0;
    }
</style>
