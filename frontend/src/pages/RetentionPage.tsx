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

type SortCol = 'accountid' | 'trade_count' | 'days_in_retention' | 'total_profit';

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

function SortIcon({ col, sortBy, sortDir }: { col: SortCol; sortBy: SortCol; sortDir: 'asc' | 'desc' }) {
  if (sortBy !== col) return <span className="ml-1 text-gray-300">↕</span>;
  return <span className="ml-1">{sortDir === 'asc' ? '↑' : '↓'}</span>;
}

export function RetentionPage() {
  const [data, setData] = useState<RetentionResponse | null>(null);
  const [page, setPage] = useState(1);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');
  const [search, setSearch] = useState('');
  const [searchInput, setSearchInput] = useState('');
  const [sortBy, setSortBy] = useState<SortCol>('accountid');
  const [sortDir, setSortDir] = useState<'asc' | 'desc'>('asc');

  const load = async (p: number, s: string, col: SortCol, dir: 'asc' | 'desc') => {
    setLoading(true);
    setError('');
    try {
      const res = await api.get('/retention/clients', {
        params: { page: p, page_size: PAGE_SIZE, search: s, sort_by: col, sort_dir: dir },
      });
      setData(res.data);
    } catch (err: any) {
      setError(err.response?.data?.detail || 'Failed to load retention data');
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => { load(page, search, sortBy, sortDir); }, [page, search, sortBy, sortDir]);

  const handleSearch = (e: React.FormEvent) => {
    e.preventDefault();
    setSearch(searchInput);
    setPage(1);
  };

  const handleSort = (col: SortCol) => {
    if (sortBy === col) {
      setSortDir((d) => (d === 'asc' ? 'desc' : 'asc'));
    } else {
      setSortBy(col);
      setSortDir('asc');
    }
    setPage(1);
  };

  const totalPages = data ? Math.ceil(data.total / PAGE_SIZE) : 0;

  const thClass = 'px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider cursor-pointer select-none hover:bg-gray-100';
  const thClassRight = 'px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase tracking-wider cursor-pointer select-none hover:bg-gray-100';

  return (
    <div className="space-y-4">
      {/* Search + stats bar */}
      <div className="flex items-center gap-4 flex-wrap">
        <form onSubmit={handleSearch} className="flex gap-2">
          <input
            type="text"
            value={searchInput}
            onChange={(e) => setSearchInput(e.target.value)}
            placeholder="Search account ID…"
            className="border border-gray-300 rounded-md px-3 py-1.5 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 w-56"
          />
          <button
            type="submit"
            className="px-3 py-1.5 bg-blue-600 text-white rounded-md text-sm font-medium hover:bg-blue-700"
          >
            Search
          </button>
          {search && (
            <button
              type="button"
              onClick={() => { setSearch(''); setSearchInput(''); setPage(1); }}
              className="px-3 py-1.5 text-sm text-gray-500 hover:text-gray-700 border border-gray-300 rounded-md"
            >
              Clear
            </button>
          )}
        </form>
        {data && !loading && (
          <p className="text-sm text-gray-500">
            {data.total.toLocaleString()} {search ? 'matching' : 'qualified'} accounts
          </p>
        )}
      </div>

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
                <th className={thClass} onClick={() => handleSort('accountid')}>
                  Account ID <SortIcon col="accountid" sortBy={sortBy} sortDir={sortDir} />
                </th>
                <th className={thClass} onClick={() => handleSort('trade_count')}>
                  Trade Count <SortIcon col="trade_count" sortBy={sortBy} sortDir={sortDir} />
                </th>
                <th className={thClass} onClick={() => handleSort('days_in_retention')}>
                  Days in Retention <SortIcon col="days_in_retention" sortBy={sortBy} sortDir={sortDir} />
                </th>
                <th className={thClassRight} onClick={() => handleSort('total_profit')}>
                  Total Profit <SortIcon col="total_profit" sortBy={sortBy} sortDir={sortDir} />
                </th>
              </tr>
            </thead>
            <tbody>
              {loading ? (
                <tr>
                  <td colSpan={4} className="px-4 py-12 text-center text-sm text-gray-400">Loading…</td>
                </tr>
              ) : !data || data.clients.length === 0 ? (
                <tr>
                  <td colSpan={4} className="px-4 py-12 text-center text-sm text-gray-400">No qualified accounts found.</td>
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
