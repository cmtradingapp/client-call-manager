import { useEffect, useState } from 'react';
import axios from 'axios';

const api = axios.create({ baseURL: import.meta.env.VITE_API_BASE_URL || '/api' });
api.interceptors.request.use((config) => {
  const auth = JSON.parse(localStorage.getItem('auth') || '{}');
  const token = auth?.state?.token;
  if (token) config.headers.Authorization = `Bearer ${token}`;
  return config;
});


type DatePreset = '' | 'today' | 'yesterday' | 'last7' | 'this_month' | 'last_month' | 'this_year' | 'last_year' | 'custom';

function getPresetDates(preset: DatePreset): { from: string; to: string } {
  const today = new Date();
  const fmt = (d: Date) => d.toISOString().slice(0, 10);
  switch (preset) {
    case 'today': return { from: fmt(today), to: fmt(today) };
    case 'yesterday': { const d = new Date(today); d.setDate(d.getDate() - 1); return { from: fmt(d), to: fmt(d) }; }
    case 'last7': { const d = new Date(today); d.setDate(d.getDate() - 6); return { from: fmt(d), to: fmt(today) }; }
    case 'this_month': return { from: fmt(new Date(today.getFullYear(), today.getMonth(), 1)), to: fmt(today) };
    case 'last_month': return { from: fmt(new Date(today.getFullYear(), today.getMonth() - 1, 1)), to: fmt(new Date(today.getFullYear(), today.getMonth(), 0)) };
    case 'this_year': return { from: fmt(new Date(today.getFullYear(), 0, 1)), to: fmt(today) };
    case 'last_year': return { from: fmt(new Date(today.getFullYear() - 1, 0, 1)), to: fmt(new Date(today.getFullYear() - 1, 11, 31)) };
    default: return { from: '', to: ''  };
  }
}

const PAGE_SIZE = 50;
type SortCol = 'accountid' | 'client_qualification_date' | 'days_in_retention' | 'trade_count' | 'total_profit' | 'last_trade_date' | 'days_from_last_trade' | 'active' | 'active_ftd' | 'deposit_count' | 'total_deposit' | 'balance' | 'credit' | 'equity' | 'open_pnl' | 'sales_client_potential' | 'age';
type NumOp = '' | 'eq' | 'gt' | 'gte' | 'lt' | 'lte';
type BoolFilter = '' | 'true' | 'false';

interface RetentionClient {
  accountid: string;
  client_qualification_date: string | null;
  days_in_retention: number | null;
  trade_count: number;
  total_profit: number;
  last_trade_date: string | null;
  days_from_last_trade: number | null;
  active: boolean;
  active_ftd: boolean;
  deposit_count: number;
  total_deposit: number;
  balance: number;
  credit: number;
  equity: number;
  open_pnl: number;
  sales_client_potential: string | null;
  age: number | null;
}

interface Filters {
  accountid: string;
  qual_date_from: string;
  qual_date_to: string;
  trade_count_op: NumOp;
  trade_count_val: string;
  days_op: NumOp;
  days_val: string;
  profit_op: NumOp;
  profit_val: string;
  last_trade_preset: DatePreset;
  last_trade_from: string;
  last_trade_to: string;
  days_from_last_trade_op: NumOp;
  days_from_last_trade_val: string;
  deposit_count_op: NumOp;
  deposit_count_val: string;
  total_deposit_op: NumOp;
  total_deposit_val: string;
  balance_op: NumOp;
  balance_val: string;
  credit_op: NumOp;
  credit_val: string;
  active: BoolFilter;
  active_ftd: BoolFilter;
}

const EMPTY_FILTERS: Filters = {
  accountid: '',
  qual_date_from: '',
  qual_date_to: '',
  trade_count_op: '',
  trade_count_val: '',
  days_op: '',
  days_val: '',
  profit_op: '',
  profit_val: '',
  last_trade_preset: '',
  last_trade_from: '',
  last_trade_to: '',
  days_from_last_trade_op: '',
  days_from_last_trade_val: '',
  deposit_count_op: '',
  deposit_count_val: '',
  total_deposit_op: '',
  total_deposit_val: '',
  balance_op: '',
  balance_val: '',
  credit_op: '',
  credit_val: '',
  active: '',
  active_ftd: '',
};

function countActive(f: Filters) {
  return [
    f.accountid,
    f.qual_date_from,
    f.qual_date_to,
    f.trade_count_op && f.trade_count_val,
    f.days_op && f.days_val,
    f.profit_op && f.profit_val,
    f.last_trade_preset === 'custom' ? f.last_trade_from : f.last_trade_preset,
    f.days_from_last_trade_op && f.days_from_last_trade_val,
    f.deposit_count_op && f.deposit_count_val,
    f.total_deposit_op && f.total_deposit_val,
    f.balance_op && f.balance_val,
    f.credit_op && f.credit_val,
    f.active,
    f.active_ftd,
  ].filter(Boolean).length;
}

function SortIcon({ col, sortBy, sortDir }: { col: SortCol; sortBy: SortCol; sortDir: 'asc' | 'desc' }) {
  if (sortBy !== col) return <span className="ml-1 text-gray-300">↕</span>;
  return <span className="ml-1">{sortDir === 'asc' ? '↑' : '↓'}</span>;
}

function NumericFilter({ label, op, val, onOp, onVal }: {
  label: string; op: NumOp; val: string;
  onOp: (v: NumOp) => void; onVal: (v: string) => void;
}) {
  return (
    <div>
      <label className="block text-xs font-medium text-gray-600 mb-1">{label}</label>
      <div className="flex gap-1">
        <select value={op} onChange={(e) => onOp(e.target.value as NumOp)} className="border border-gray-300 rounded-md px-2 py-1.5 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 bg-white">
          <option value="">—</option>
          <option value="eq">= Equal</option>
          <option value="gt">&gt; Greater</option>
          <option value="gte">≥ At least</option>
          <option value="lt">&lt; Less</option>
          <option value="lte">≤ At most</option>
        </select>
        <input type="number" value={val} onChange={(e) => onVal(e.target.value)} disabled={!op} placeholder="Value"
          className="border border-gray-300 rounded-md px-2 py-1.5 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 w-28 disabled:bg-gray-50 disabled:text-gray-400" />
      </div>
    </div>
  );
}

function BoolSelect({ label, value, onChange }: { label: string; value: BoolFilter; onChange: (v: BoolFilter) => void }) {
  return (
    <div>
      <label className="block text-xs font-medium text-gray-600 mb-1">{label}</label>
      <select value={value} onChange={(e) => onChange(e.target.value as BoolFilter)}
        className="border border-gray-300 rounded-md px-2 py-1.5 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 bg-white w-full">
        <option value="">Any</option>
        <option value="true">Yes</option>
        <option value="false">No</option>
      </select>
    </div>
  );
}

function BoolBadge({ value }: { value: boolean }) {
  return value
    ? <span className="px-2 py-0.5 rounded-full text-xs font-medium bg-green-100 text-green-700">Yes</span>
    : <span className="px-2 py-0.5 rounded-full text-xs font-medium bg-gray-100 text-gray-500">No</span>;
}

function formatDate(iso: string | null) {
  if (!iso) return '—';
  return new Date(iso).toLocaleDateString();
}

function fmtNum(v: number, decimals = 2) {
  return v.toLocaleString(undefined, { minimumFractionDigits: decimals, maximumFractionDigits: decimals });
}

export function RetentionPage() {
  const [data, setData] = useState<{ total: number; clients: RetentionClient[] } | null>(null);
  const [page, setPage] = useState(1);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');
  const [sortBy, setSortBy] = useState<SortCol>('accountid');
  const [sortDir, setSortDir] = useState<'asc' | 'desc'>('asc');
  const [filtersOpen, setFiltersOpen] = useState(false);
  const [draft, setDraft] = useState<Filters>(EMPTY_FILTERS);
  const [applied, setApplied] = useState<Filters>(EMPTY_FILTERS);
  const [activityDays, setActivityDays] = useState('35');

  const load = async (p: number, col: SortCol, dir: 'asc' | 'desc', f: Filters, actDays: string) => {
    setLoading(true);
    setError('');
    try {
      const res = await api.get('/retention/clients', {
        params: {
          page: p, page_size: PAGE_SIZE, sort_by: col, sort_dir: dir,
          accountid: f.accountid,
          qual_date_from: f.qual_date_from || undefined,
          qual_date_to: f.qual_date_to || undefined,
          trade_count_op: f.trade_count_op, trade_count_val: f.trade_count_val || undefined,
          days_op: f.days_op, days_val: f.days_val || undefined,
          profit_op: f.profit_op, profit_val: f.profit_val || undefined,
          last_trade_from: f.last_trade_from || undefined,
          last_trade_to: f.last_trade_to || undefined,
          days_from_last_trade_op: f.days_from_last_trade_op, days_from_last_trade_val: f.days_from_last_trade_val || undefined,
          deposit_count_op: f.deposit_count_op, deposit_count_val: f.deposit_count_val || undefined,
          total_deposit_op: f.total_deposit_op, total_deposit_val: f.total_deposit_val || undefined,
          balance_op: f.balance_op, balance_val: f.balance_val || undefined,
          credit_op: f.credit_op, credit_val: f.credit_val || undefined,
          active: f.active, active_ftd: f.active_ftd,
          activity_days: actDays || 35,
        },
      });
      setData(res.data);
    } catch (err: any) {
      setError(err.response?.data?.detail || 'Failed to load retention data');
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => { load(page, sortBy, sortDir, applied, activityDays); }, [page, sortBy, sortDir, applied, activityDays]);

  const handleSort = (col: SortCol) => {
    if (sortBy === col) setSortDir((d) => (d === 'asc' ? 'desc' : 'asc'));
    else { setSortBy(col); setSortDir('asc'); }
    setPage(1);
  };

  const applyFilters = () => { setApplied({ ...draft }); setPage(1); setActivityDays(activityDays); };
  const clearFilters = () => { setDraft(EMPTY_FILTERS); setApplied(EMPTY_FILTERS); setPage(1); };
  const setField = <K extends keyof Filters>(key: K, val: Filters[K]) => setDraft((prev) => ({ ...prev, [key]: val }));

  const totalPages = data ? Math.ceil(data.total / PAGE_SIZE) : 0;
  const activeCount = countActive(applied);

  const thClass = 'px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider cursor-pointer select-none hover:bg-gray-100 whitespace-nowrap';
  const thClassRight = 'px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase tracking-wider cursor-pointer select-none hover:bg-gray-100 whitespace-nowrap';

  return (
    <div className="space-y-4">
      {/* Collapsible filters */}
      <div className="bg-white rounded-lg shadow overflow-hidden">
        <button onClick={() => setFiltersOpen((v) => !v)} className="w-full px-4 py-3 flex items-center justify-between hover:bg-gray-50 transition-colors">
          <div className="flex items-center gap-2">
            <span className="text-sm font-medium text-gray-700">Filters</span>
            {activeCount > 0 && <span className="bg-blue-100 text-blue-700 text-xs font-semibold px-2 py-0.5 rounded-full">{activeCount} active</span>}
          </div>
          <span className="text-gray-400 text-xs">{filtersOpen ? '▲ Hide' : '▼ Show'}</span>
        </button>

        {filtersOpen && (
          <div className="px-4 pb-4 pt-2 border-t border-gray-100 space-y-4">
            <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 gap-4">
              {/* Account ID */}
              <div>
                <label className="block text-xs font-medium text-gray-600 mb-1">Account ID</label>
                <input type="text" value={draft.accountid} onChange={(e) => setField('accountid', e.target.value)}
                  placeholder="Contains…" className="w-full border border-gray-300 rounded-md px-2 py-1.5 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500" />
              </div>

              {/* Qualification Date */}
              <div className="sm:col-span-2">
                <label className="block text-xs font-medium text-gray-600 mb-1">Qualification Date</label>
                <div className="flex items-center gap-2">
                  <input type="date" value={draft.qual_date_from} onChange={(e) => setField('qual_date_from', e.target.value)}
                    className="border border-gray-300 rounded-md px-2 py-1.5 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500" />
                  <span className="text-xs text-gray-400">to</span>
                  <input type="date" value={draft.qual_date_to} onChange={(e) => setField('qual_date_to', e.target.value)}
                    className="border border-gray-300 rounded-md px-2 py-1.5 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500" />
                </div>
              </div>

              {/* Last Trade Date */}
              <div className="sm:col-span-2">
                <label className="block text-xs font-medium text-gray-600 mb-1">Last Trade Date</label>
                <div className="flex items-center gap-2 flex-wrap">
                  <select
                    value={draft.last_trade_preset}
                    onChange={(e) => {
                      const p = e.target.value as DatePreset;
                      if (p === '' || p === 'custom') {
                        setDraft((prev) => ({ ...prev, last_trade_preset: p, last_trade_from: '', last_trade_to: '' }));
                      } else {
                        const dates = getPresetDates(p);
                        setDraft((prev) => ({ ...prev, last_trade_preset: p, last_trade_from: dates.from, last_trade_to: dates.to }));
                      }
                    }}
                    className="border border-gray-300 rounded-md px-2 py-1.5 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 bg-white"
                  >
                    <option value="">Any</option>
                    <option value="today">Today</option>
                    <option value="yesterday">Yesterday</option>
                    <option value="last7">Last 7 days</option>
                    <option value="this_month">This month</option>
                    <option value="last_month">Last month</option>
                    <option value="this_year">This year</option>
                    <option value="last_year">Last year</option>
                    <option value="custom">Custom</option>
                  </select>
                  {draft.last_trade_preset === 'custom' && (
                    <>
                      <input type="date" value={draft.last_trade_from} onChange={(e) => setField('last_trade_from', e.target.value)}
                        className="border border-gray-300 rounded-md px-2 py-1.5 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500" />
                      <span className="text-xs text-gray-400">to</span>
                      <input type="date" value={draft.last_trade_to} onChange={(e) => setField('last_trade_to', e.target.value)}
                        className="border border-gray-300 rounded-md px-2 py-1.5 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500" />
                    </>
                  )}
                  {draft.last_trade_preset && draft.last_trade_preset !== 'custom' && draft.last_trade_from && (
                    <span className="text-xs text-gray-500 bg-gray-100 px-2 py-1 rounded">{draft.last_trade_from} → {draft.last_trade_to}</span>
                  )}
                </div>
              </div>

              <NumericFilter label="Days in Retention" op={draft.days_op} val={draft.days_val}
                onOp={(v) => setField('days_op', v)} onVal={(v) => setField('days_val', v)} />
              <NumericFilter label="Trade Count" op={draft.trade_count_op} val={draft.trade_count_val}
                onOp={(v) => setField('trade_count_op', v)} onVal={(v) => setField('trade_count_val', v)} />
              <NumericFilter label="Total Profit" op={draft.profit_op} val={draft.profit_val}
                onOp={(v) => setField('profit_op', v)} onVal={(v) => setField('profit_val', v)} />
              <NumericFilter label="Days from Last Trade" op={draft.days_from_last_trade_op} val={draft.days_from_last_trade_val}
                onOp={(v) => setField('days_from_last_trade_op', v)} onVal={(v) => setField('days_from_last_trade_val', v)} />
              <NumericFilter label="Deposit Count" op={draft.deposit_count_op} val={draft.deposit_count_val}
                onOp={(v) => setField('deposit_count_op', v)} onVal={(v) => setField('deposit_count_val', v)} />
              <NumericFilter label="Total Deposit" op={draft.total_deposit_op} val={draft.total_deposit_val}
                onOp={(v) => setField('total_deposit_op', v)} onVal={(v) => setField('total_deposit_val', v)} />
              <NumericFilter label="Balance" op={draft.balance_op} val={draft.balance_val}
                onOp={(v) => setField('balance_op', v)} onVal={(v) => setField('balance_val', v)} />
              <NumericFilter label="Credit" op={draft.credit_op} val={draft.credit_val}
                onOp={(v) => setField('credit_op', v)} onVal={(v) => setField('credit_val', v)} />
              {/* Activity window */}
              <div>
                <label className="block text-xs font-medium text-gray-600 mb-1">Activity Window (days)</label>
                <input
                  type="number" min={1} max={365} value={activityDays}
                  onChange={(e) => setActivityDays(e.target.value)}
                  className="border border-gray-300 rounded-md px-2 py-1.5 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 w-28"
                />
                <p className="text-xs text-gray-400 mt-0.5">Default: 35</p>
              </div>
              <BoolSelect label={`Active (trade/deposit in last ${activityDays || 35}d)`} value={draft.active} onChange={(v) => setField('active', v)} />
              <BoolSelect label="Active FTD" value={draft.active_ftd} onChange={(v) => setField('active_ftd', v)} />
            </div>
            <div className="flex gap-2">
              <button onClick={applyFilters} className="px-4 py-1.5 bg-blue-600 text-white rounded-md text-sm font-medium hover:bg-blue-700">Apply Filters</button>
              <button onClick={clearFilters} className="px-4 py-1.5 border border-gray-300 text-gray-600 rounded-md text-sm font-medium hover:bg-gray-50">Clear All</button>
            </div>
          </div>
        )}
      </div>

      {/* Table */}
      <div className="bg-white rounded-lg shadow overflow-hidden">
        <div className="px-4 py-3 border-b border-gray-200 bg-gray-50 flex items-center justify-between">
          <span className="text-sm text-gray-600">
            {loading ? 'Loading…' : `${data?.total?.toLocaleString() ?? 0} accounts${activeCount > 0 ? ' (filtered)' : ''} — showing ${((page - 1) * PAGE_SIZE) + 1}–${Math.min(page * PAGE_SIZE, data?.total ?? 0)}`}
          </span>
          {totalPages > 1 && (
            <div className="flex items-center gap-2">
              <button onClick={() => setPage((p) => Math.max(1, p - 1))} disabled={page === 1 || loading} className="px-3 py-1 text-xs rounded-md border border-gray-300 disabled:opacity-40 hover:bg-gray-50">← Prev</button>
              <span className="text-xs text-gray-500">{page} / {totalPages}</span>
              <button onClick={() => setPage((p) => Math.min(totalPages, p + 1))} disabled={page === totalPages || loading} className="px-3 py-1 text-xs rounded-md border border-gray-300 disabled:opacity-40 hover:bg-gray-50">Next →</button>
            </div>
          )}
        </div>

        {error && <div className="px-4 py-3 bg-red-50 border-b border-red-100 text-sm text-red-600">{error}</div>}

        <div className="overflow-x-auto">
          <table className="w-full">
            <thead className="bg-gray-50 sticky top-0">
              <tr>
                <th className={thClass} onClick={() => handleSort('accountid')}>Account ID <SortIcon col="accountid" sortBy={sortBy} sortDir={sortDir} /></th>
                <th className={thClass} onClick={() => handleSort('sales_client_potential')}>Potential <SortIcon col="sales_client_potential" sortBy={sortBy} sortDir={sortDir} /></th>
                <th className={thClass} onClick={() => handleSort('age')}>Age <SortIcon col="age" sortBy={sortBy} sortDir={sortDir} /></th>
                <th className={thClass} onClick={() => handleSort('client_qualification_date')}>Qual. Date <SortIcon col="client_qualification_date" sortBy={sortBy} sortDir={sortDir} /></th>
                <th className={thClass} onClick={() => handleSort('days_in_retention')}>Days in Ret. <SortIcon col="days_in_retention" sortBy={sortBy} sortDir={sortDir} /></th>
                <th className={thClass} onClick={() => handleSort('trade_count')}>Trades <SortIcon col="trade_count" sortBy={sortBy} sortDir={sortDir} /></th>
                <th className={thClassRight} onClick={() => handleSort('total_profit')}>Total Profit <SortIcon col="total_profit" sortBy={sortBy} sortDir={sortDir} /></th>
                <th className={thClass} onClick={() => handleSort('last_trade_date')}>Last Trade <SortIcon col="last_trade_date" sortBy={sortBy} sortDir={sortDir} /></th>
                <th className={thClass} onClick={() => handleSort('days_from_last_trade')}>Days from Last Trade <SortIcon col="days_from_last_trade" sortBy={sortBy} sortDir={sortDir} /></th>
                <th className={thClass} onClick={() => handleSort('deposit_count')}>Deposits <SortIcon col="deposit_count" sortBy={sortBy} sortDir={sortDir} /></th>
                <th className={thClassRight} onClick={() => handleSort('total_deposit')}>Total Deposit <SortIcon col="total_deposit" sortBy={sortBy} sortDir={sortDir} /></th>
                <th className={thClassRight} onClick={() => handleSort('balance')}>Balance <SortIcon col="balance" sortBy={sortBy} sortDir={sortDir} /></th>
                <th className={thClassRight} onClick={() => handleSort('credit')}>Credit <SortIcon col="credit" sortBy={sortBy} sortDir={sortDir} /></th>
                <th className={thClassRight} onClick={() => handleSort('equity')}>Equity <SortIcon col="equity" sortBy={sortBy} sortDir={sortDir} /></th>
                <th className={thClassRight} onClick={() => handleSort('open_pnl')}>Open PNL <SortIcon col="open_pnl" sortBy={sortBy} sortDir={sortDir} /></th>
                <th className={thClass} onClick={() => handleSort('active')}>Active <SortIcon col="active" sortBy={sortBy} sortDir={sortDir} /></th>
                <th className={thClass} onClick={() => handleSort('active_ftd')}>Active FTD <SortIcon col="active_ftd" sortBy={sortBy} sortDir={sortDir} /></th>
              </tr>
            </thead>
            <tbody>
              {loading ? (
                <tr><td colSpan={17} className="px-4 py-12 text-center text-sm text-gray-400">Loading…</td></tr>
              ) : !data || data.clients.length === 0 ? (
                <tr><td colSpan={17} className="px-4 py-12 text-center text-sm text-gray-400">No accounts found.</td></tr>
              ) : (
                data.clients.map((c) => (
                  <tr key={c.accountid} className="border-t border-gray-100 hover:bg-gray-50">
                    <td className="px-4 py-3 text-sm font-medium">
                      <a href={`https://crm.cmtrading.com/#/users/user/${c.accountid}`} target="_blank" rel="noreferrer" className="text-blue-600 hover:underline">{c.accountid}</a>
                    </td>
                    <td className="px-4 py-3 text-sm text-gray-700">{c.sales_client_potential ?? '—'}</td>
                    <td className="px-4 py-3 text-sm text-gray-700">{c.age ?? '—'}</td>
                    <td className="px-4 py-3 text-sm text-gray-700">{formatDate(c.client_qualification_date)}</td>
                    <td className="px-4 py-3 text-sm text-gray-700">{c.days_in_retention ?? '—'}</td>
                    <td className="px-4 py-3 text-sm text-gray-700">{c.trade_count.toLocaleString()}</td>
                    <td className={`px-4 py-3 text-sm text-right font-medium ${c.total_profit >= 0 ? 'text-green-600' : 'text-red-600'}`}>
                      {fmtNum(c.total_profit)}
                    </td>
                    <td className="px-4 py-3 text-sm text-gray-700">{formatDate(c.last_trade_date)}</td>
                    <td className="px-4 py-3 text-sm text-gray-700">{c.days_from_last_trade ?? '—'}</td>
                    <td className="px-4 py-3 text-sm text-gray-700">{c.deposit_count.toLocaleString()}</td>
                    <td className="px-4 py-3 text-sm text-right text-gray-700">{fmtNum(c.total_deposit)}</td>
                    <td className="px-4 py-3 text-sm text-right text-gray-700">{fmtNum(c.balance)}</td>
                    <td className="px-4 py-3 text-sm text-right text-gray-700">{fmtNum(c.credit)}</td>
                    <td className="px-4 py-3 text-sm text-right text-gray-700">{fmtNum(c.equity)}</td>
                    <td className={`px-4 py-3 text-sm text-right font-medium ${c.open_pnl >= 0 ? 'text-green-600' : 'text-red-600'}`}>{fmtNum(c.open_pnl)}</td>
                    <td className="px-4 py-3"><BoolBadge value={c.active} /></td>
                    <td className="px-4 py-3"><BoolBadge value={c.active_ftd} /></td>
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
