import { useEffect, useRef, useState } from 'react';
import axios from 'axios';

const api = axios.create({ baseURL: import.meta.env.VITE_API_BASE_URL || '/api' });
api.interceptors.request.use((config) => {
  const auth = JSON.parse(localStorage.getItem('auth') || '{}');
  const token = auth?.state?.token;
  if (token) config.headers.Authorization = `Bearer ${token}`;
  return config;
});

interface SyncLog {
  id: number;
  sync_type: 'full' | 'incremental';
  status: 'running' | 'completed' | 'error';
  started_at: string | null;
  completed_at: string | null;
  rows_synced: number | null;
  error_message: string | null;
}

function StatusBadge({ status }: { status: SyncLog['status'] }) {
  const styles = {
    running: 'bg-yellow-100 text-yellow-800',
    completed: 'bg-green-100 text-green-800',
    error: 'bg-red-100 text-red-800',
  };
  return (
    <span className={`px-2 py-0.5 rounded-full text-xs font-medium ${styles[status]}`}>
      {status === 'running' ? '⟳ Running' : status === 'completed' ? '✓ Completed' : '✗ Error'}
    </span>
  );
}

function formatDate(iso: string | null) {
  if (!iso) return '—';
  return new Date(iso).toLocaleString();
}

function duration(log: SyncLog) {
  if (!log.started_at || !log.completed_at) return '—';
  const ms = new Date(log.completed_at).getTime() - new Date(log.started_at).getTime();
  const s = Math.round(ms / 1000);
  return s < 60 ? `${s}s` : `${Math.floor(s / 60)}m ${s % 60}s`;
}

export function ETLPage() {
  const [logs, setLogs] = useState<SyncLog[]>([]);
  const [syncing, setSyncing] = useState(false);
  const [error, setError] = useState('');
  const intervalRef = useRef<ReturnType<typeof setInterval> | null>(null);

  const fetchStatus = async () => {
    try {
      const res = await api.get('/etl/sync-status');
      setLogs(res.data.logs);
    } catch {
      // silently ignore polling errors
    }
  };

  useEffect(() => {
    fetchStatus();
    intervalRef.current = setInterval(fetchStatus, 10_000);
    return () => { if (intervalRef.current) clearInterval(intervalRef.current); };
  }, []);

  const runFullSync = async () => {
    setSyncing(true);
    setError('');
    try {
      await api.post('/etl/sync-trades');
      await fetchStatus();
    } catch (e: any) {
      setError(e.response?.data?.detail || 'Failed to start sync');
    } finally {
      setSyncing(false);
    }
  };

  const hasRunning = logs.some((l) => l.status === 'running');
  const lastCompleted = logs.find((l) => l.status === 'completed');

  return (
    <div className="space-y-6">
      {/* Status cards */}
      <div className="grid grid-cols-1 sm:grid-cols-3 gap-4">
        <div className="bg-white rounded-lg shadow px-5 py-4">
          <p className="text-xs text-gray-500 mb-1">Last Completed</p>
          <p className="text-sm font-semibold text-gray-800">{formatDate(lastCompleted?.completed_at ?? null)}</p>
        </div>
        <div className="bg-white rounded-lg shadow px-5 py-4">
          <p className="text-xs text-gray-500 mb-1">Last Rows Synced</p>
          <p className="text-2xl font-bold text-gray-800">{lastCompleted?.rows_synced?.toLocaleString() ?? '—'}</p>
        </div>
        <div className="bg-white rounded-lg shadow px-5 py-4">
          <p className="text-xs text-gray-500 mb-1">Cron Schedule</p>
          <p className="text-sm font-semibold text-gray-800">Every 5 minutes (incremental)</p>
        </div>
      </div>

      {/* Full sync button */}
      <div className="bg-white rounded-lg shadow p-5">
        <h2 className="text-sm font-semibold text-gray-700 mb-1">Full Sync</h2>
        <p className="text-xs text-gray-500 mb-4">
          Truncates the local <code className="bg-gray-100 px-1 rounded">trades_mt4</code> table and re-imports all rows
          from the dealio replica. Use this for the initial load or to fix data issues.
          The sync runs in the background — check the log below for progress.
        </p>
        {error && <p className="text-sm text-red-600 mb-3">{error}</p>}
        <button
          onClick={runFullSync}
          disabled={syncing || hasRunning}
          className="px-5 py-2 bg-blue-600 text-white rounded-md text-sm font-medium hover:bg-blue-700 disabled:opacity-50 transition-colors"
        >
          {syncing ? 'Starting…' : hasRunning ? 'Sync Running…' : 'Run Full Sync'}
        </button>
      </div>

      {/* Log table */}
      <div className="bg-white rounded-lg shadow overflow-hidden">
        <div className="px-4 py-3 border-b border-gray-200 bg-gray-50 flex items-center justify-between">
          <span className="text-sm font-medium text-gray-700">Sync History</span>
          <span className="text-xs text-gray-400">Auto-refreshes every 10s</span>
        </div>
        <div className="overflow-x-auto">
          <table className="w-full">
            <thead className="bg-gray-50">
              <tr>
                {['Type', 'Status', 'Started', 'Completed', 'Duration', 'Rows Synced', 'Error'].map((h) => (
                  <th key={h} className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                    {h}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {logs.length === 0 ? (
                <tr>
                  <td colSpan={7} className="px-4 py-10 text-center text-sm text-gray-400">No sync history yet.</td>
                </tr>
              ) : (
                logs.map((log) => (
                  <tr key={log.id} className="border-t border-gray-100 hover:bg-gray-50">
                    <td className="px-4 py-3 text-sm font-medium text-gray-800 capitalize">{log.sync_type}</td>
                    <td className="px-4 py-3"><StatusBadge status={log.status} /></td>
                    <td className="px-4 py-3 text-sm text-gray-600 whitespace-nowrap">{formatDate(log.started_at)}</td>
                    <td className="px-4 py-3 text-sm text-gray-600 whitespace-nowrap">{formatDate(log.completed_at)}</td>
                    <td className="px-4 py-3 text-sm text-gray-600">{duration(log)}</td>
                    <td className="px-4 py-3 text-sm text-gray-700">{log.rows_synced?.toLocaleString() ?? '—'}</td>
                    <td className="px-4 py-3 text-xs text-red-600 max-w-xs truncate">{log.error_message ?? '—'}</td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}
