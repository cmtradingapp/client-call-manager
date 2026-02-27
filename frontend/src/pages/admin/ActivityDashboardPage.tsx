import { useEffect, useState, useCallback } from 'react';
import axios from 'axios';

const api = axios.create({ baseURL: import.meta.env.VITE_API_BASE_URL || '/api' });
api.interceptors.request.use((config) => {
  const auth = JSON.parse(localStorage.getItem('auth') || '{}');
  const token = auth?.state?.token;
  if (token) config.headers.Authorization = `Bearer ${token}`;
  return config;
});

interface AgentStats {
  agent_username: string;
  calls: number;
  notes: number;
  status_changes: number;
  whatsapp: number;
  total: number;
}

interface DashboardResponse {
  periods: {
    today: AgentStats[];
    this_week: AgentStats[];
    this_month: AgentStats[];
  };
  last_updated: string;
}

type PeriodKey = 'today' | 'this_week' | 'this_month';

const TABS: { key: PeriodKey; label: string }[] = [
  { key: 'today', label: 'Today' },
  { key: 'this_week', label: 'This Week' },
  { key: 'this_month', label: 'This Month' },
];

const REFRESH_INTERVAL = 60_000; // 60 seconds

export function ActivityDashboardPage() {
  const [data, setData] = useState<DashboardResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');
  const [activeTab, setActiveTab] = useState<PeriodKey>('today');

  const fetchData = useCallback(async (showSpinner = false) => {
    if (showSpinner) setLoading(true);
    setError('');
    try {
      const res = await api.get<DashboardResponse>('/admin/activity-dashboard');
      setData(res.data);
    } catch (err: any) {
      setError(err.response?.data?.detail || 'Failed to load activity data');
    } finally {
      setLoading(false);
    }
  }, []);

  // Initial fetch + auto-refresh every 60 seconds
  useEffect(() => {
    fetchData(true);
    const interval = setInterval(() => fetchData(false), REFRESH_INTERVAL);
    return () => clearInterval(interval);
  }, [fetchData]);

  const agents = data?.periods[activeTab] || [];

  if (loading) {
    return (
      <div className="flex items-center justify-center py-20">
        <div className="flex flex-col items-center gap-3">
          <div className="h-8 w-8 animate-spin rounded-full border-4 border-blue-600 border-t-transparent" />
          <span className="text-sm text-gray-500">Loading activity data...</span>
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="bg-red-50 border border-red-200 rounded-lg p-6 text-center">
        <p className="text-sm text-red-700">{error}</p>
        <button
          onClick={() => fetchData(true)}
          className="mt-3 px-4 py-1.5 bg-red-600 text-white rounded-md text-sm font-medium hover:bg-red-700 transition-colors"
        >
          Retry
        </button>
      </div>
    );
  }

  return (
    <div className="space-y-4">
      {/* Header with last-updated */}
      <div className="flex items-center justify-between">
        <p className="text-sm text-gray-500">
          Agent performance leaderboard sourced from audit log
        </p>
        {data?.last_updated && (
          <p className="text-xs text-gray-400">
            Last updated: {new Date(data.last_updated).toLocaleTimeString()}
            <span className="ml-1 text-gray-300">(auto-refreshes every 60s)</span>
          </p>
        )}
      </div>

      {/* Tabs */}
      <div className="border-b border-gray-200">
        <nav className="-mb-px flex gap-6">
          {TABS.map((tab) => (
            <button
              key={tab.key}
              onClick={() => setActiveTab(tab.key)}
              className={`pb-3 px-1 text-sm font-medium border-b-2 transition-colors ${
                activeTab === tab.key
                  ? 'border-blue-600 text-blue-600'
                  : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300'
              }`}
            >
              {tab.label}
              {data && (
                <span className={`ml-2 px-1.5 py-0.5 rounded-full text-xs ${
                  activeTab === tab.key ? 'bg-blue-100 text-blue-700' : 'bg-gray-100 text-gray-500'
                }`}>
                  {data.periods[tab.key].length}
                </span>
              )}
            </button>
          ))}
        </nav>
      </div>

      {/* Leaderboard Table */}
      {agents.length === 0 ? (
        <div className="bg-white rounded-lg shadow p-12 text-center">
          <div className="text-gray-400 text-4xl mb-3">--</div>
          <p className="text-sm text-gray-500 font-medium">No activity yet</p>
          <p className="text-xs text-gray-400 mt-1">
            Agent actions will appear here once logged in the audit trail
          </p>
        </div>
      ) : (
        <div className="bg-white rounded-lg shadow overflow-hidden">
          <div className="overflow-x-auto">
            <table className="w-full">
              <thead className="bg-gray-50">
                <tr>
                  {['Rank', 'Agent', 'Calls', 'Notes', 'Status Changes', 'WhatsApp', 'Total'].map(
                    (h) => (
                      <th
                        key={h}
                        className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider"
                      >
                        {h}
                      </th>
                    ),
                  )}
                </tr>
              </thead>
              <tbody>
                {agents.map((agent, idx) => {
                  const isTop = idx === 0;
                  return (
                    <tr
                      key={agent.agent_username}
                      className={
                        isTop
                          ? 'bg-amber-50 border-b border-amber-100'
                          : 'border-b border-gray-100 hover:bg-gray-50'
                      }
                    >
                      <td className="px-4 py-3 text-sm">
                        {isTop ? (
                          <span className="inline-flex items-center justify-center w-6 h-6 rounded-full bg-amber-400 text-white text-xs font-bold">
                            1
                          </span>
                        ) : (
                          <span className="inline-flex items-center justify-center w-6 h-6 rounded-full bg-gray-200 text-gray-600 text-xs font-medium">
                            {idx + 1}
                          </span>
                        )}
                      </td>
                      <td className={`px-4 py-3 text-sm font-medium ${isTop ? 'text-amber-900' : 'text-gray-900'}`}>
                        {agent.agent_username}
                      </td>
                      <td className="px-4 py-3 text-sm text-gray-600 tabular-nums">{agent.calls}</td>
                      <td className="px-4 py-3 text-sm text-gray-600 tabular-nums">{agent.notes}</td>
                      <td className="px-4 py-3 text-sm text-gray-600 tabular-nums">{agent.status_changes}</td>
                      <td className="px-4 py-3 text-sm text-gray-600 tabular-nums">{agent.whatsapp}</td>
                      <td className="px-4 py-3 text-sm tabular-nums">
                        <span
                          className={`inline-block px-2.5 py-0.5 rounded-full text-xs font-semibold ${
                            isTop
                              ? 'bg-amber-200 text-amber-900'
                              : 'bg-gray-100 text-gray-700'
                          }`}
                        >
                          {agent.total}
                        </span>
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </div>
  );
}
