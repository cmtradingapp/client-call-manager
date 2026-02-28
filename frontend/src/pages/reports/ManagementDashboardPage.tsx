import { useEffect, useState, useCallback } from 'react';
import axios from 'axios';
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
} from 'recharts';

// ---------------------------------------------------------------------------
// Axios instance — same auth pattern as other pages
// ---------------------------------------------------------------------------
const api = axios.create({ baseURL: import.meta.env.VITE_API_BASE_URL || '/api' });
api.interceptors.request.use((config) => {
  const auth = JSON.parse(localStorage.getItem('auth') || '{}');
  const token = auth?.state?.token;
  if (token) config.headers.Authorization = `Bearer ${token}`;
  return config;
});

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------
interface AgentCallRow {
  agent_id: string;
  agent_name: string;
  total_calls: number;
  success_count: number;
  failure_count: number;
  unknown_count: number;
}

interface CallsDashboard {
  agents: AgentCallRow[];
  total_calls: number;
}

interface AgentActivity {
  agent_username: string;
  calls: number;
  notes: number;
  status_changes: number;
  whatsapp: number;
  total: number;
}

interface ActivityDashboard {
  periods: {
    today: AgentActivity[];
    this_week: AgentActivity[];
    this_month: AgentActivity[];
  };
  last_updated: string;
}

interface SyncLastRow {
  id: string;
  modified: string | null;
}

interface SyncLog {
  id: number;
  sync_type: string;
  status: string;
  started_at: string | null;
  completed_at: string | null;
  rows_synced: number | null;
  error_message: string | null;
}

interface SyncStatus {
  ant_acc_row_count: number;
  vta_row_count: number;
  mtt_row_count: number;
  extensions_row_count: number;
  vtiger_campaigns_row_count: number;
  vtiger_users_row_count: number;
  trades_row_count: number;
  dealio_users_row_count: number;
  open_pnl_row_count: number;
  ant_acc_last: SyncLastRow | null;
  vta_last: SyncLastRow | null;
  mtt_last: SyncLastRow | null;
  extensions_last: SyncLastRow | null;
  vtiger_campaigns_last: SyncLastRow | null;
  vtiger_users_last: SyncLastRow | null;
  trades_last: SyncLastRow | null;
  dealio_users_last: SyncLastRow | null;
  open_pnl_last: SyncLastRow | null;
  logs: SyncLog[];
}

interface CallHistoryConversation {
  conversation_id: string;
  agent_id: string;
  agent_name?: string;
  call_duration_secs?: number;
  call_successful?: string;
  start_time_unix_secs?: number;
  status?: string;
}

interface CallHistoryResponse {
  conversations: CallHistoryConversation[];
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------
function formatDuration(secs: number | undefined): string {
  if (!secs) return '0s';
  const m = Math.floor(secs / 60);
  const s = secs % 60;
  return m > 0 ? `${m}m ${s}s` : `${s}s`;
}

function formatTime(unix: number | undefined): string {
  if (!unix) return '--';
  const d = new Date(unix * 1000);
  return d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
}

function truncate(s: string, max: number): string {
  return s.length > max ? s.slice(0, max) + '...' : s;
}

const REFRESH_INTERVAL = 60_000;

// ---------------------------------------------------------------------------
// Skeleton components
// ---------------------------------------------------------------------------
function CardSkeleton() {
  return (
    <div className="bg-white rounded-lg shadow p-5 animate-pulse">
      <div className="flex items-center gap-3">
        <div className="w-10 h-10 rounded-lg bg-gray-200" />
        <div className="flex-1 space-y-2">
          <div className="h-6 w-16 bg-gray-200 rounded" />
          <div className="h-3 w-24 bg-gray-100 rounded" />
        </div>
      </div>
    </div>
  );
}

function ChartSkeleton() {
  return (
    <div className="bg-white rounded-lg shadow p-5 animate-pulse">
      <div className="h-4 w-40 bg-gray-200 rounded mb-4" />
      <div className="h-64 bg-gray-100 rounded" />
    </div>
  );
}

function TableSkeleton() {
  return (
    <div className="bg-white rounded-lg shadow overflow-hidden animate-pulse">
      <div className="h-4 w-36 bg-gray-200 rounded m-5" />
      <div className="space-y-2 px-5 pb-5">
        {[...Array(5)].map((_, i) => (
          <div key={i} className="h-8 bg-gray-100 rounded" />
        ))}
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// KPI Card
// ---------------------------------------------------------------------------
interface KpiCardProps {
  icon: string;
  iconBg: string;
  iconColor: string;
  value: string | number;
  label: string;
}

function KpiCard({ icon, iconBg, iconColor, value, label }: KpiCardProps) {
  return (
    <div className="bg-white rounded-lg shadow p-5 flex items-center gap-4">
      <div className={`w-11 h-11 rounded-lg flex items-center justify-center text-lg ${iconBg} ${iconColor}`}>
        {icon}
      </div>
      <div>
        <p className="text-2xl font-bold text-gray-900">{value}</p>
        <p className="text-xs text-gray-500">{label}</p>
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Sync table config
// ---------------------------------------------------------------------------
interface SyncTableRow {
  name: string;
  key: string;
  rowCountKey: string;
  lastKey: string;
}

const SYNC_TABLE_ROWS: SyncTableRow[] = [
  { name: 'Client Accounts (ant_acc)', key: 'ant_acc', rowCountKey: 'ant_acc_row_count', lastKey: 'ant_acc_last' },
  { name: 'Trading Accounts (vta)', key: 'vta', rowCountKey: 'vta_row_count', lastKey: 'vta_last' },
  { name: 'Transactions (mtt)', key: 'mtt', rowCountKey: 'mtt_row_count', lastKey: 'mtt_last' },
  { name: 'Extensions', key: 'extensions', rowCountKey: 'extensions_row_count', lastKey: 'extensions_last' },
  { name: 'Vtiger Campaigns', key: 'vtiger_campaigns', rowCountKey: 'vtiger_campaigns_row_count', lastKey: 'vtiger_campaigns_last' },
  { name: 'Vtiger Users', key: 'vtiger_users', rowCountKey: 'vtiger_users_row_count', lastKey: 'vtiger_users_last' },
  { name: 'Trades (MT4)', key: 'trades', rowCountKey: 'trades_row_count', lastKey: 'trades_last' },
  { name: 'Dealio Users', key: 'dealio_users', rowCountKey: 'dealio_users_row_count', lastKey: 'dealio_users_last' },
  { name: 'Open PnL Cache', key: 'open_pnl', rowCountKey: 'open_pnl_row_count', lastKey: 'open_pnl_last' },
];

// ---------------------------------------------------------------------------
// Main Component
// ---------------------------------------------------------------------------
export function ManagementDashboardPage() {
  const [loading, setLoading] = useState(true);
  const [lastUpdated, setLastUpdated] = useState<string>('');

  // Data states
  const [callsDash, setCallsDash] = useState<CallsDashboard | null>(null);
  const [activityDash, setActivityDash] = useState<ActivityDashboard | null>(null);
  const [syncStatus, setSyncStatus] = useState<SyncStatus | null>(null);
  const [callHistory, setCallHistory] = useState<CallHistoryConversation[]>([]);
  const [retentionActive, setRetentionActive] = useState<number>(0);
  const [retentionTotal, setRetentionTotal] = useState<number>(0);

  const fetchAll = useCallback(async (showSpinner = false) => {
    if (showSpinner) setLoading(true);
    try {
      const [callsRes, activityRes, syncRes, historyRes, activeRes, totalRes] = await Promise.allSettled([
        api.get<CallsDashboard>('/calls/dashboard', { params: { days: 30 } }),
        api.get<ActivityDashboard>('/admin/activity-dashboard'),
        api.get<SyncStatus>('/etl/sync-status'),
        api.get<CallHistoryResponse>('/calls/history', { params: { page_size: 20 } }),
        api.get('/retention/clients', { params: { active: 'true', page_size: 1 } }),
        api.get('/retention/clients', { params: { page_size: 1 } }),
      ]);

      if (callsRes.status === 'fulfilled') setCallsDash(callsRes.value.data);
      if (activityRes.status === 'fulfilled') setActivityDash(activityRes.value.data);
      if (syncRes.status === 'fulfilled') setSyncStatus(syncRes.value.data);
      if (historyRes.status === 'fulfilled') setCallHistory(historyRes.value.data.conversations || []);
      if (activeRes.status === 'fulfilled') setRetentionActive(activeRes.value.data.total ?? 0);
      if (totalRes.status === 'fulfilled') setRetentionTotal(totalRes.value.data.total ?? 0);

      setLastUpdated(new Date().toLocaleTimeString());
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchAll(true);
    const interval = setInterval(() => fetchAll(false), REFRESH_INTERVAL);
    return () => clearInterval(interval);
  }, [fetchAll]);

  // ---------------------------------------------------------------------------
  // Computed KPI values
  // ---------------------------------------------------------------------------
  const totalCalls = callsDash?.total_calls ?? 0;

  const totalSuccess = callsDash?.agents.reduce((s, a) => s + a.success_count, 0) ?? 0;
  const successRate = totalCalls > 0 ? ((totalSuccess / totalCalls) * 100).toFixed(1) : '0.0';

  const todayActions = activityDash?.periods.today.reduce((s, a) => s + a.total, 0) ?? 0;

  // Sync health: check the latest log entry per sync type
  const syncHealthy = (() => {
    if (!syncStatus?.logs?.length) return false;
    const thirtyMinAgo = Date.now() - 30 * 60 * 1000;
    // Check if any sync completed within last 30 min
    return syncStatus.logs.some((log) => {
      if (log.status !== 'success') return false;
      if (!log.completed_at) return false;
      return new Date(log.completed_at).getTime() > thirtyMinAgo;
    });
  })();

  // ---------------------------------------------------------------------------
  // Chart data
  // ---------------------------------------------------------------------------
  const callsByAgentData = (callsDash?.agents || []).map((a) => ({
    name: truncate(a.agent_name, 25),
    Success: a.success_count,
    Failure: a.failure_count,
    Unknown: a.unknown_count,
  }));

  const activityByAgentData = (activityDash?.periods.today || []).map((a) => ({
    name: truncate(a.agent_username, 20),
    Calls: a.calls,
    Notes: a.notes,
    'Status Changes': a.status_changes,
    WhatsApp: a.whatsapp,
  }));

  // ---------------------------------------------------------------------------
  // Render
  // ---------------------------------------------------------------------------
  if (loading) {
    return (
      <div className="space-y-6">
        <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-4">
          {[...Array(6)].map((_, i) => <CardSkeleton key={i} />)}
        </div>
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
          <ChartSkeleton />
          <ChartSkeleton />
        </div>
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
          <TableSkeleton />
          <TableSkeleton />
        </div>
      </div>
    );
  }

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <p className="text-sm text-gray-500">
          Overview of AI calls, retention clients, agent activity, and data sync health
        </p>
        {lastUpdated && (
          <p className="text-xs text-gray-400">
            Last updated: {lastUpdated}
            <span className="ml-1 text-gray-300">(auto-refreshes every 60s)</span>
          </p>
        )}
      </div>

      {/* KPI Cards */}
      <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-4">
        <KpiCard
          icon="&#9742;"
          iconBg="bg-blue-100"
          iconColor="text-blue-600"
          value={totalCalls.toLocaleString()}
          label="Total AI Calls (30d)"
        />
        <KpiCard
          icon="%"
          iconBg="bg-green-100"
          iconColor="text-green-600"
          value={`${successRate}%`}
          label="Call Success Rate"
        />
        <KpiCard
          icon="&#9899;"
          iconBg="bg-orange-100"
          iconColor="text-orange-600"
          value={retentionActive.toLocaleString()}
          label="Active Retention Clients"
        />
        <KpiCard
          icon="&#9881;"
          iconBg="bg-purple-100"
          iconColor="text-purple-600"
          value={retentionTotal.toLocaleString()}
          label="Total Retention Clients"
        />
        <KpiCard
          icon="&#9889;"
          iconBg="bg-teal-100"
          iconColor="text-teal-600"
          value={todayActions.toLocaleString()}
          label="Today's Agent Actions"
        />
        <KpiCard
          icon={syncHealthy ? '\u2713' : '\u26A0'}
          iconBg={syncHealthy ? 'bg-green-100' : 'bg-red-100'}
          iconColor={syncHealthy ? 'text-green-600' : 'text-red-600'}
          value={syncHealthy ? 'Live' : 'Stale'}
          label="Data Sync Health"
        />
      </div>

      {/* Charts */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Chart 1: AI Calls by Agent */}
        <div className="bg-white rounded-lg shadow p-5">
          <h3 className="text-sm font-semibold text-gray-700 mb-4">AI Calls by Agent (Last 30 Days)</h3>
          {callsByAgentData.length === 0 ? (
            <div className="h-64 flex items-center justify-center text-sm text-gray-400">No call data available</div>
          ) : (
            <ResponsiveContainer width="100%" height={300}>
              <BarChart data={callsByAgentData} layout="vertical" margin={{ left: 20, right: 20 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" />
                <XAxis type="number" tick={{ fontSize: 11 }} />
                <YAxis dataKey="name" type="category" tick={{ fontSize: 11 }} width={140} />
                <Tooltip contentStyle={{ fontSize: 12 }} />
                <Legend wrapperStyle={{ fontSize: 12 }} />
                <Bar dataKey="Success" stackId="a" fill="#22c55e" />
                <Bar dataKey="Failure" stackId="a" fill="#ef4444" />
                <Bar dataKey="Unknown" stackId="a" fill="#9ca3af" />
              </BarChart>
            </ResponsiveContainer>
          )}
        </div>

        {/* Chart 2: Today's Activity by Agent */}
        <div className="bg-white rounded-lg shadow p-5">
          <h3 className="text-sm font-semibold text-gray-700 mb-4">Today's Activity by Agent</h3>
          {activityByAgentData.length === 0 ? (
            <div className="h-64 flex items-center justify-center text-sm text-gray-400">No activity data yet today</div>
          ) : (
            <ResponsiveContainer width="100%" height={300}>
              <BarChart data={activityByAgentData} margin={{ left: 10, right: 20 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" />
                <XAxis dataKey="name" tick={{ fontSize: 11 }} />
                <YAxis tick={{ fontSize: 11 }} />
                <Tooltip contentStyle={{ fontSize: 12 }} />
                <Legend wrapperStyle={{ fontSize: 12 }} />
                <Bar dataKey="Calls" fill="#3b82f6" />
                <Bar dataKey="Notes" fill="#22c55e" />
                <Bar dataKey="Status Changes" fill="#f97316" />
                <Bar dataKey="WhatsApp" fill="#8b5cf6" />
              </BarChart>
            </ResponsiveContainer>
          )}
        </div>
      </div>

      {/* Tables */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Table 1: Recent Calls */}
        <div className="bg-white rounded-lg shadow overflow-hidden">
          <div className="px-5 py-4 border-b border-gray-100">
            <h3 className="text-sm font-semibold text-gray-700">Recent Calls (Last 20)</h3>
          </div>
          <div className="overflow-x-auto">
            <table className="w-full">
              <thead className="bg-gray-50">
                <tr>
                  {['Agent', 'Direction', 'Duration', 'Status', 'Time'].map((h) => (
                    <th
                      key={h}
                      className="px-4 py-2.5 text-left text-xs font-medium text-gray-500 uppercase tracking-wider"
                    >
                      {h}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {callHistory.length === 0 ? (
                  <tr>
                    <td colSpan={5} className="px-4 py-8 text-center text-sm text-gray-400">
                      No recent calls
                    </td>
                  </tr>
                ) : (
                  callHistory.slice(0, 20).map((c, idx) => (
                    <tr key={c.conversation_id || idx} className="border-b border-gray-50 hover:bg-gray-50">
                      <td className="px-4 py-2.5 text-sm text-gray-900">
                        {truncate(c.agent_name || c.agent_id || '--', 20)}
                      </td>
                      <td className="px-4 py-2.5 text-sm text-gray-600">
                        Outbound
                      </td>
                      <td className="px-4 py-2.5 text-sm text-gray-600 tabular-nums">
                        {formatDuration(c.call_duration_secs)}
                      </td>
                      <td className="px-4 py-2.5">
                        <span
                          className={`inline-block px-2 py-0.5 rounded-full text-xs font-medium ${
                            c.call_successful === 'success'
                              ? 'bg-green-100 text-green-700'
                              : c.call_successful === 'failure'
                              ? 'bg-red-100 text-red-700'
                              : 'bg-gray-100 text-gray-600'
                          }`}
                        >
                          {c.call_successful || c.status || 'unknown'}
                        </span>
                      </td>
                      <td className="px-4 py-2.5 text-sm text-gray-500 tabular-nums">
                        {formatTime(c.start_time_unix_secs)}
                      </td>
                    </tr>
                  ))
                )}
              </tbody>
            </table>
          </div>
        </div>

        {/* Table 2: ETL Sync Status */}
        <div className="bg-white rounded-lg shadow overflow-hidden">
          <div className="px-5 py-4 border-b border-gray-100">
            <h3 className="text-sm font-semibold text-gray-700">ETL Sync Status</h3>
          </div>
          <div className="overflow-x-auto">
            <table className="w-full">
              <thead className="bg-gray-50">
                <tr>
                  {['Data Source', 'Last Sync', 'Rows', 'Status'].map((h) => (
                    <th
                      key={h}
                      className="px-4 py-2.5 text-left text-xs font-medium text-gray-500 uppercase tracking-wider"
                    >
                      {h}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {!syncStatus ? (
                  <tr>
                    <td colSpan={4} className="px-4 py-8 text-center text-sm text-gray-400">
                      No sync data
                    </td>
                  </tr>
                ) : (
                  SYNC_TABLE_ROWS.map((row) => {
                    const rowCount = (syncStatus as any)[row.rowCountKey] ?? 0;
                    const lastObj: SyncLastRow | null = (syncStatus as any)[row.lastKey] ?? null;
                    const lastModified = lastObj?.modified;
                    const isRecent = lastModified
                      ? Date.now() - new Date(lastModified).getTime() < 60 * 60 * 1000
                      : false;
                    const hasData = rowCount > 0;

                    return (
                      <tr key={row.key} className="border-b border-gray-50 hover:bg-gray-50">
                        <td className="px-4 py-2.5 text-sm text-gray-900">{row.name}</td>
                        <td className="px-4 py-2.5 text-sm text-gray-600 tabular-nums">
                          {lastModified
                            ? new Date(lastModified).toLocaleString([], {
                                month: 'short',
                                day: 'numeric',
                                hour: '2-digit',
                                minute: '2-digit',
                              })
                            : '--'}
                        </td>
                        <td className="px-4 py-2.5 text-sm text-gray-600 tabular-nums">
                          {rowCount.toLocaleString()}
                        </td>
                        <td className="px-4 py-2.5">
                          {hasData ? (
                            <span
                              className={`inline-block px-2 py-0.5 rounded-full text-xs font-medium ${
                                isRecent
                                  ? 'bg-green-100 text-green-700'
                                  : 'bg-yellow-100 text-yellow-700'
                              }`}
                            >
                              {isRecent ? '\u2713 Synced' : '\u26A0 Stale'}
                            </span>
                          ) : (
                            <span className="inline-block px-2 py-0.5 rounded-full text-xs font-medium bg-red-100 text-red-700">
                              No data
                            </span>
                          )}
                        </td>
                      </tr>
                    );
                  })
                )}
              </tbody>
            </table>
          </div>
        </div>
      </div>
    </div>
  );
}
