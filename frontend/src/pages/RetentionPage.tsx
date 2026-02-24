import { useEffect, useState } from 'react';
import axios from 'axios';

const api = axios.create({ baseURL: import.meta.env.VITE_API_BASE_URL || '/api' });
api.interceptors.request.use((config) => {
  const auth = JSON.parse(localStorage.getItem('auth') || '{}');
  const token = auth?.state?.token;
  if (token) config.headers.Authorization = `Bearer ${token}`;
  return config;
});

const PAGE_SIZE = 50;

interface RetentionClient {
  accountid: string;
  trade_count: number;
  days_in_retention: number | null;
  total_profit: number;
}

interface RetentionResponse {
  total: number;
  page: number;
  page_size: number;
  clients: RetentionClient[];
}

export function RetentionPage() {
  const [data, setData] = useState<RetentionResponse | null>(null);
  const [page, setPage] = useState(1);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');

  const load = async (p: number) => {
    setLoading(true);
    setError('');
    try {
      const res = await api.get('/retention/clients', {
        params: { page: p, page_size: PAGE_SIZE },
      });
      setData(res.data);
    } catch (err: any) {
      setError(err.response?.data?.detail || 'Failed to load retention data');
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => { load(page); }, [page]);

  const totalPages = data ? Math.ceil(data.total / PAGE_SIZE) : 0;

  return (
    <div className="space-y-4">
      {/* Stats bar */}
      {data && !loading && (
        <div className="bg-white rounded-lg shadow px-5 py-3 flex items-center gap-6">
          <div>
            <p className="text-2xl font-bold text-gray-800">{data.total.toLocaleString()}</p>
            <p className="text-xs text-gray-500">Qualified Accounts</p>
          </div>
        </div>
      )}

      <div className="bg-white rounded-lg shadow overflow-hidden">
        <div className="px-4 py-3 border-b border-gray-200 bg-gray-50 flex items-center justify-between">
          <span className="text-sm text-gray-600">
            {loading ? 'Loading…' : `Showing ${((page - 1) * PAGE_SIZE) + 1}–${Math.min(page * PAGE_SIZE, data?.total ?? 0)} of ${data?.total?.toLocaleString() ?? 0}`}
          </span>
          {totalPages > 1 && (
            <div className="flex items-center gap-2">
              <button
                onClick={() => setPage((p) => Math.max(1, p - 1))}
                disabled={page === 1 || loading}
                className="px-3 py-1 text-xs rounded-md border border-gray-300 disabled:opacity-40 hover:bg-gray-50"
              >
                ← Prev
              </button>
              <span className="text-xs text-gray-500">{page} / {totalPages}</span>
              <button
                onClick={() => setPage((p) => Math.min(totalPages, p + 1))}
                disabled={page === totalPages || loading}
                className="px-3 py-1 text-xs rounded-md border border-gray-300 disabled:opacity-40 hover:bg-gray-50"
              >
                Next →
              </button>
            </div>
          )}
        </div>

        {error && (
          <div className="px-4 py-3 bg-red-50 border-b border-red-100 text-sm text-red-600">
            {error}
          </div>
        )}

        <div className="overflow-x-auto">
          <table className="w-full">
            <thead className="bg-gray-50 sticky top-0">
              <tr>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Account ID</th>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Trade Count</th>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Days in Retention</th>
                <th className="px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase tracking-wider">Total Profit</th>
              </tr>
            </thead>
            <tbody>
              {loading ? (
                <tr>
                  <td colSpan={4} className="px-4 py-12 text-center text-sm text-gray-400">
                    Loading…
                  </td>
                </tr>
              ) : !data || data.clients.length === 0 ? (
                <tr>
                  <td colSpan={4} className="px-4 py-12 text-center text-sm text-gray-400">
                    No qualified accounts found.
                  </td>
                </tr>
              ) : (
                data.clients.map((c) => (
                  <tr key={c.accountid} className="border-t border-gray-100 hover:bg-gray-50">
                    <td className="px-4 py-3 text-sm font-medium text-gray-900">{c.accountid}</td>
                    <td className="px-4 py-3 text-sm text-gray-700">{c.trade_count.toLocaleString()}</td>
                    <td className="px-4 py-3 text-sm text-gray-700">{c.days_in_retention ?? '—'}</td>
                    <td className={`px-4 py-3 text-sm text-right font-medium ${c.total_profit >= 0 ? 'text-green-600' : 'text-red-600'}`}>
                      {c.total_profit.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}
                    </td>
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
