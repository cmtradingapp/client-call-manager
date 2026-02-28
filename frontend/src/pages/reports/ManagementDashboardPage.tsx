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
interface TradingDashboard {
  today_deposits: { count: number; amount_usd: number };
  today_ftds: { count: number; amount_usd: number };
  today_withdrawals: { count: number; amount_usd: number };
  net_deposits_today: number;
  total_client_balance: number;
  total_client_credit: number;
  total_accounts: number;
  active_traders_today: number;
  retention_clients_total: number;
  retention_clients_active: number;
  retention_mv_ready: boolean;
  top_depositors_today: Array<{
    login: string;
    name: string;
    amount_usd: number;
    payment_method: string;
  }>;
  deposits_last_7_days: Array<{
    date: string;
    count: number;
    amount_usd: number;
  }>;
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

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------
function formatAmount(n: number | undefined): string {
  if (n === undefined || n === null) return '--';
  return Math.abs(n).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
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
  valueColor?: string;
}

function KpiCard({ icon, iconBg, iconColor, value, label, valueColor }: KpiCardProps) {
  return (
    <div className="bg-white rounded-lg shadow p-5 flex items-center gap-4">
      <div className={`w-11 h-11 rounded-lg flex items-center justify-center text-lg ${iconBg} ${iconColor}`}>
        {icon}
      </div>
      <div>
        <p className={`text-2xl font-bold ${valueColor ?? 'text-gray-900'}`}>{value}</p>
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
  const [lastUpdated, setLastUpdated] = useState<string>('');

  // Per-section loading states
  const [tradingLoading, setTradingLoading] = useState(true);
  const [syncLoading, setSyncLoading] = useState(true);

  // Data states
  const [trading, setTrading] = useState<TradingDashboard | null>(null);
  const [syncStatus, setSyncStatus] = useState<SyncStatus | null>(null);

  const fetchAll = useCallback((showSpinners = false) => {
    if (showSpinners) {
      setTradingLoading(true);
      setSyncLoading(true);
    }

    api.get<TradingDashboard>('/dashboard/trading')
      .then(r => setTrading(r.data))
      .catch(() => {})
      .finally(() => setTradingLoading(false));

    api.get<SyncStatus>('/etl/sync-status')
      .then(r => setSyncStatus(r.data))
      .catch(() => {})
      .finally(() => setSyncLoading(false));

    setLastUpdated(new Date().toLocaleTimeString());
  }, []);

  useEffect(() => {
    fetchAll(true);
    const interval = setInterval(() => fetchAll(false), REFRESH_INTERVAL);
    return () => clearInterval(interval);
  }, [fetchAll]);

  // ---------------------------------------------------------------------------
  // Computed values
  // ---------------------------------------------------------------------------
  const syncHealthy = (() => {
    if (!syncStatus?.logs?.length) return false;
    const thirtyMinAgo = Date.now() - 30 * 60 * 1000;
    return syncStatus.logs.some((log) => {
      if (log.status !== 'success') return false;
      if (!log.completed_at) return false;
      return new Date(log.completed_at).getTime() > thirtyMinAgo;
    });
  })();

  const netDeposits = trading?.net_deposits_today;
  const netDepositsPositive = netDeposits !== undefined && netDeposits >= 0;
  const netDepositsSign = netDeposits !== undefined ? (netDeposits >= 0 ? '+' : '-') : '';
  const netDepositsColor = netDeposits !== undefined
    ? (netDeposits >= 0 ? 'text-green-600' : 'text-red-600')
    : 'text-gray-900';

  // ---------------------------------------------------------------------------
  // Chart data
  // ---------------------------------------------------------------------------
  const depositsChartData = (trading?.deposits_last_7_days || []).map((d) => ({
    date: new Date(d.date + 'T00:00:00').toLocaleDateString('en-US', { month: 'short', day: 'numeric' }),
    'Amount USD': d.amount_usd,
    Count: d.count,
  }));

  // ---------------------------------------------------------------------------
  // Render
  // ---------------------------------------------------------------------------
  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <p className="text-sm text-gray-500">
          Overview of trading activity, retention clients, and data sync health
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
        {tradingLoading ? (
          <>
            <CardSkeleton />
            <CardSkeleton />
            <CardSkeleton />
            <CardSkeleton />
            <CardSkeleton />
          </>
        ) : (
          <>
            <KpiCard
              icon="💰"
              iconBg="bg-blue-100"
              iconColor="text-blue-600"
              value={trading?.today_deposits.count ?? '--'}
              label="Deposits Today"
            />
            <KpiCard
              icon="📈"
              iconBg="bg-green-100"
              iconColor="text-green-600"
              value={`$${formatAmount(trading?.today_deposits.amount_usd)}`}
              label="Deposit Volume (USD)"
            />
            <KpiCard
              icon="⭐"
              iconBg="bg-amber-100"
              iconColor="text-amber-600"
              value={trading?.today_ftds.count ?? '--'}
              label="First-Time Deposits"
            />
            <KpiCard
              icon="📊"
              iconBg="bg-teal-100"
              iconColor="text-teal-600"
              value={trading?.active_traders_today ?? '--'}
              label="Active Traders Today"
            />
            <KpiCard
              icon="⚖️"
              iconBg={netDepositsPositive ? 'bg-green-100' : 'bg-red-100'}
              iconColor={netDepositsPositive ? 'text-green-600' : 'text-red-600'}
              value={netDeposits !== undefined ? `${netDepositsSign}$${formatAmount(netDeposits)}` : '--'}
              label="Net Deposits (USD)"
              valueColor={netDepositsColor}
            />
          </>
        )}
        {syncLoading ? (
          <CardSkeleton />
        ) : (
          <KpiCard
            icon={syncHealthy ? '\u2713' : '\u26A0'}
            iconBg={syncHealthy ? 'bg-green-100' : 'bg-red-100'}
            iconColor={syncHealthy ? 'text-green-600' : 'text-red-600'}
            value={syncHealthy ? 'Live' : 'Stale'}
            label="Data Sync"
          />
        )}
      </div>

      {/* Info Bar */}
      {trading !== null && (
        <div className="bg-gray-50 border border-gray-200 rounded-lg px-5 py-3 text-sm text-gray-600 flex flex-wrap gap-x-0 gap-y-1">
          <span>
            Total Accounts: <strong>{trading.total_accounts.toLocaleString()}</strong>
          </span>
          <span className="mx-3 text-gray-300">|</span>
          <span>
            Total Balance: <strong>${formatAmount(trading.total_client_balance)}</strong>
          </span>
          <span className="mx-3 text-gray-300">|</span>
          {trading.retention_mv_ready ? (
            <>
              <span>
                Retention Clients: <strong>{trading.retention_clients_total.toLocaleString()}</strong>
              </span>
              <span className="mx-3 text-gray-300">|</span>
              <span>
                Active: <strong>{trading.retention_clients_active.toLocaleString()}</strong>
              </span>
            </>
          ) : (
            <span className="text-gray-400 italic">Retention: loading...</span>
          )}
        </div>
      )}

      {/* Charts — single full-width chart */}
      <div className="grid grid-cols-1 gap-6">
        {tradingLoading ? (
          <ChartSkeleton />
        ) : (
          <div className="bg-white rounded-lg shadow p-5">
            <h3 className="text-sm font-semibold text-gray-700 mb-4">Deposits Last 7 Days</h3>
            {depositsChartData.length === 0 ? (
              <div className="h-64 flex items-center justify-center text-sm text-gray-400">No deposit data available</div>
            ) : (
              <ResponsiveContainer width="100%" height={300}>
                <BarChart data={depositsChartData} margin={{ left: 10, right: 40 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" />
                  <XAxis dataKey="date" tick={{ fontSize: 11 }} />
                  <YAxis yAxisId="left" label={{ value: 'Amount USD', angle: -90, position: 'insideLeft', offset: -5, style: { fontSize: 11 } }} tick={{ fontSize: 11 }} />
                  <YAxis yAxisId="right" orientation="right" label={{ value: 'Count', angle: 90, position: 'insideRight', offset: 5, style: { fontSize: 11 } }} tick={{ fontSize: 11 }} />
                  <Tooltip contentStyle={{ fontSize: 12 }} />
                  <Legend wrapperStyle={{ fontSize: 12 }} />
                  <Bar yAxisId="left" dataKey="Amount USD" fill="#22c55e" />
                  <Bar yAxisId="right" dataKey="Count" fill="#3b82f6" />
                </BarChart>
              </ResponsiveContainer>
            )}
          </div>
        )}
      </div>

      {/* Tables */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Table 1: Top Depositors Today */}
        {tradingLoading ? (
          <TableSkeleton />
        ) : (
          <div className="bg-white rounded-lg shadow overflow-hidden">
            <div className="px-5 py-4 border-b border-gray-100">
              <h3 className="text-sm font-semibold text-gray-700">Top Depositors Today</h3>
            </div>
            <div className="overflow-x-auto">
              <table className="w-full">
                <thead className="bg-gray-50">
                  <tr>
                    {['#', 'Name', 'Amount (USD)', 'Method'].map((h) => (
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
                  {(trading?.top_depositors_today || []).length === 0 ? (
                    <tr>
                      <td colSpan={4} className="px-4 py-8 text-center text-sm text-gray-400">
                        No deposits recorded today
                      </td>
                    </tr>
                  ) : (
                    (trading?.top_depositors_today || []).map((depositor, idx) => (
                      <tr key={depositor.login || idx} className="border-b border-gray-50 hover:bg-gray-50">
                        <td className="px-4 py-2.5 text-sm text-gray-500 tabular-nums font-medium">
                          #{idx + 1}
                        </td>
                        <td className="px-4 py-2.5 text-sm text-gray-900">
                          {depositor.name || depositor.login}
                        </td>
                        <td className="px-4 py-2.5 text-sm text-gray-900 tabular-nums font-medium">
                          ${formatAmount(depositor.amount_usd)}
                        </td>
                        <td className="px-4 py-2.5 text-sm text-gray-600">
                          {depositor.payment_method || '--'}
                        </td>
                      </tr>
                    ))
                  )}
                </tbody>
              </table>
            </div>
          </div>
        )}

        {/* Table 2: ETL Sync Status */}
        {syncLoading ? (
          <TableSkeleton />
        ) : (
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
        )}
      </div>
    </div>
  );
}
