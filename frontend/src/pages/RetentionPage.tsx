import { useEffect, useState, useRef, useCallback, useMemo, ReactNode } from 'react';
import { useVirtualizer } from '@tanstack/react-virtual';
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
type SortCol = 'accountid' | 'full_name' | 'client_qualification_date' | 'days_in_retention' | 'trade_count' | 'total_profit' | 'last_trade_date' | 'days_from_last_trade' | 'active' | 'active_ftd' | 'deposit_count' | 'total_deposit' | 'balance' | 'credit' | 'equity' | 'open_pnl' | 'live_equity' | 'max_open_trade' | 'max_volume' | 'turnover' | 'win_rate' | 'avg_trade_size' | 'sales_client_potential' | 'age' | 'agent_name' | 'score';
type NumOp = '' | 'eq' | 'gt' | 'gte' | 'lt' | 'lte';
type BoolFilter = '' | 'true' | 'false';

// ── Per-column header filters ──────────────────────────────────────────────
type ColNumOp = 'gt' | 'lt' | 'eq' | 'gte' | 'lte' | 'between';
type ColDatePreset = 'today' | 'this_week' | 'this_month' | 'custom';

type ColFilter =
  | { type: 'text'; value: string }
  | { type: 'numeric'; op: ColNumOp; val: string; val2?: string }
  | { type: 'date'; preset?: ColDatePreset; from?: string; to?: string };

type ColFilters = Partial<Record<string, ColFilter>>;

interface TaskInfo {
  name: string;
  color: string;
}

interface RetentionClient {
  accountid: string;
  full_name: string;
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
  live_equity: number;
  max_open_trade: number | null;
  max_volume: number | null;
  turnover: number;
  win_rate: number | null;
  avg_trade_size: number | null;
  assigned_to: string | null;
  agent_name: string | null;
  tasks: TaskInfo[];
  score: number;
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
  equity_op: NumOp;
  equity_val: string;
  live_equity_op: NumOp;
  live_equity_val: string;
  max_open_trade_op: NumOp;
  max_open_trade_val: string;
  max_volume_op: NumOp;
  max_volume_val: string;
  turnover_op: NumOp;
  turnover_val: string;
  assigned_to: string;
  task_id: string;
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
  equity_op: '',
  equity_val: '',
  live_equity_op: '',
  live_equity_val: '',
  max_open_trade_op: '',
  max_open_trade_val: '',
  max_volume_op: '',
  max_volume_val: '',
  turnover_op: '',
  turnover_val: '',
  assigned_to: '',
  task_id: '',
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
    f.equity_op && f.equity_val,
    f.live_equity_op && f.live_equity_val,
    f.max_open_trade_op && f.max_open_trade_val,
    f.max_volume_op && f.max_volume_val,
    f.turnover_op && f.turnover_val,
    f.assigned_to,
    f.task_id,
    f.active,
    f.active_ftd,
  ].filter(Boolean).length;
}

// ── ColFilter helper components ───────────────────────────────────────────

function ColTextFilter({ col, colFilters, setColFilters }: {
  col: string;
  colFilters: ColFilters;
  setColFilters: React.Dispatch<React.SetStateAction<ColFilters>>;
}) {
  const filter = colFilters[col];
  const value = filter?.type === 'text' ? filter.value : '';

  const handleChange = (v: string) => {
    setColFilters((prev) => {
      if (!v) {
        const next = { ...prev };
        delete next[col];
        return next;
      }
      return { ...prev, [col]: { type: 'text', value: v } };
    });
  };

  return (
    <div className="flex items-center gap-0.5 mt-1" onClick={(e) => e.stopPropagation()}>
      <input
        type="text"
        value={value}
        onChange={(e) => handleChange(e.target.value)}
        placeholder="Filter..."
        className="w-full min-w-0 border border-gray-300 rounded px-1.5 py-0.5 text-xs focus:outline-none focus:ring-1 focus:ring-blue-400 bg-white"
      />
      {value && (
        <button
          onClick={() => handleChange('')}
          className="text-gray-400 hover:text-gray-600 text-xs leading-none flex-shrink-0 ml-0.5"
          title="Clear"
        >
          x
        </button>
      )}
    </div>
  );
}

function ColNumericFilter({ col, colFilters, setColFilters }: {
  col: string;
  colFilters: ColFilters;
  setColFilters: React.Dispatch<React.SetStateAction<ColFilters>>;
}) {
  const filter = colFilters[col];
  const op: ColNumOp = (filter?.type === 'numeric' ? filter.op : 'gt') as ColNumOp;
  const val = filter?.type === 'numeric' ? filter.val : '';
  const val2 = filter?.type === 'numeric' ? (filter.val2 ?? '') : '';

  const update = (newOp: ColNumOp, newVal: string, newVal2?: string) => {
    setColFilters((prev) => {
      if (!newVal) {
        const next = { ...prev };
        delete next[col];
        return next;
      }
      const entry: ColFilter = { type: 'numeric', op: newOp, val: newVal };
      if (newOp === 'between' && newVal2) (entry as { type: 'numeric'; op: ColNumOp; val: string; val2?: string }).val2 = newVal2;
      return { ...prev, [col]: entry };
    });
  };

  return (
    <div className="mt-1 space-y-0.5" onClick={(e) => e.stopPropagation()}>
      <div className="flex items-center gap-0.5">
        <select
          value={op}
          onChange={(e) => update(e.target.value as ColNumOp, val, val2)}
          className="border border-gray-300 rounded px-1 py-0.5 text-xs focus:outline-none focus:ring-1 focus:ring-blue-400 bg-white flex-shrink-0"
        >
          <option value="gt">&gt;</option>
          <option value="lt">&lt;</option>
          <option value="eq">=</option>
          <option value="gte">&ge;</option>
          <option value="lte">&le;</option>
          <option value="between">btw</option>
        </select>
        <input
          type="number"
          value={val}
          onChange={(e) => update(op, e.target.value, val2)}
          placeholder="Value"
          className="min-w-0 w-16 border border-gray-300 rounded px-1.5 py-0.5 text-xs focus:outline-none focus:ring-1 focus:ring-blue-400 bg-white"
        />
        {val && (
          <button
            onClick={() => update(op, '', '')}
            className="text-gray-400 hover:text-gray-600 text-xs leading-none flex-shrink-0"
            title="Clear"
          >
            x
          </button>
        )}
      </div>
      {op === 'between' && val && (
        <input
          type="number"
          value={val2}
          onChange={(e) => update(op, val, e.target.value)}
          placeholder="To"
          className="w-full border border-gray-300 rounded px-1.5 py-0.5 text-xs focus:outline-none focus:ring-1 focus:ring-blue-400 bg-white"
        />
      )}
    </div>
  );
}

function ColDateFilter({ col, colFilters, setColFilters }: {
  col: string;
  colFilters: ColFilters;
  setColFilters: React.Dispatch<React.SetStateAction<ColFilters>>;
}) {
  const filter = colFilters[col];
  const preset = filter?.type === 'date' ? (filter.preset ?? '') : '';
  const from = filter?.type === 'date' ? (filter.from ?? '') : '';
  const to = filter?.type === 'date' ? (filter.to ?? '') : '';

  const today = new Date().toISOString().slice(0, 10);
  const getPresetRange = (p: ColDatePreset): { from: string; to: string } => {
    const now = new Date();
    const fmt = (d: Date) => d.toISOString().slice(0, 10);
    switch (p) {
      case 'today': return { from: today, to: today };
      case 'this_week': {
        const day = now.getDay();
        const monday = new Date(now); monday.setDate(now.getDate() - ((day + 6) % 7));
        return { from: fmt(monday), to: today };
      }
      case 'this_month': return { from: fmt(new Date(now.getFullYear(), now.getMonth(), 1)), to: today };
      default: return { from: '', to: '' };
    }
  };

  const update = (p: string, f?: string, t?: string) => {
    setColFilters((prev) => {
      if (!p) {
        const next = { ...prev };
        delete next[col];
        return next;
      }
      if (p === 'custom') {
        return { ...prev, [col]: { type: 'date', preset: 'custom', from: f ?? '', to: t ?? '' } };
      }
      const range = getPresetRange(p as ColDatePreset);
      return { ...prev, [col]: { type: 'date', preset: p as ColDatePreset, from: range.from, to: range.to } };
    });
  };

  return (
    <div className="mt-1 space-y-0.5" onClick={(e) => e.stopPropagation()}>
      <select
        value={preset}
        onChange={(e) => update(e.target.value)}
        className="w-full border border-gray-300 rounded px-1 py-0.5 text-xs focus:outline-none focus:ring-1 focus:ring-blue-400 bg-white"
      >
        <option value="">Any</option>
        <option value="today">Today</option>
        <option value="this_week">This Week</option>
        <option value="this_month">This Month</option>
        <option value="custom">Custom</option>
      </select>
      {preset === 'custom' && (
        <div className="flex flex-col gap-0.5">
          <input
            type="date"
            value={from}
            onChange={(e) => update('custom', e.target.value, to)}
            className="w-full border border-gray-300 rounded px-1 py-0.5 text-xs focus:outline-none focus:ring-1 focus:ring-blue-400 bg-white"
          />
          <input
            type="date"
            value={to}
            onChange={(e) => update('custom', from, e.target.value)}
            className="w-full border border-gray-300 rounded px-1 py-0.5 text-xs focus:outline-none focus:ring-1 focus:ring-blue-400 bg-white"
          />
        </div>
      )}
    </div>
  );
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

// Retention statuses from CRM (status_key -> display value)
const RETENTION_STATUSES: { key: number; label: string }[] = [
  { key: 0, label: 'New' },
  { key: 1, label: 'CallBack' },
  { key: 2, label: 'Invalid' },
  { key: 3, label: 'No Answer' },
  { key: 4, label: 'Reassign - Has Potential' },
  { key: 5, label: 'Reassign - No Potential' },
  { key: 6, label: 'Not Interested' },
  { key: 7, label: 'Under 18' },
  { key: 8, label: 'Wrong Language' },
  { key: 9, label: 'Deposited With Me' },
  { key: 10, label: 'Sessions Only' },
  { key: 11, label: 'Recovery' },
  { key: 12, label: 'Depositor' },
  { key: 13, label: 'Received Withdrawal' },
  { key: 14, label: 'Never Answers' },
  { key: 15, label: 'AvailableInNinja' },
  { key: 17, label: 'Recycle' },
  { key: 18, label: 'Never answer' },
  { key: 19, label: 'Potential' },
  { key: 20, label: 'Appointment' },
  { key: 21, label: 'High Potential' },
  { key: 22, label: 'Reshuffle' },
  { key: 23, label: 'Call Again' },
  { key: 24, label: 'Low potential' },
  { key: 25, label: 'Auto Trading' },
  { key: 26, label: 'No Balance' },
  { key: 27, label: 'IB' },
  { key: 28, label: 'Reassigned' },
  { key: 29, label: 'Language barrier' },
  { key: 30, label: 'Potential IB' },
  { key: 32, label: 'Wrong Details' },
  { key: 33, label: "Don't want assistance" },
  { key: 34, label: 'Terminated/Complain/Legal' },
  { key: 35, label: 'Remove From my Portfolio' },
  { key: 36, label: 'Daily Trading with me' },
  { key: 37, label: 'A+ Client' },
];

const TASK_COLOR_STYLES: Record<string, { bg: string; text: string }> = {
  red:    { bg: 'bg-red-100',    text: 'text-red-700' },
  orange: { bg: 'bg-orange-100', text: 'text-orange-700' },
  yellow: { bg: 'bg-yellow-100', text: 'text-yellow-700' },
  green:  { bg: 'bg-green-100',  text: 'text-green-700' },
  blue:   { bg: 'bg-blue-100',   text: 'text-blue-700' },
  purple: { bg: 'bg-purple-100', text: 'text-purple-700' },
  pink:   { bg: 'bg-pink-100',   text: 'text-pink-700' },
  grey:   { bg: 'bg-gray-100',   text: 'text-gray-700' },
};

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

function WhatsAppIcon({ className }: { className?: string }) {
  return (
    <svg className={className} viewBox="0 0 24 24" fill="currentColor" xmlns="http://www.w3.org/2000/svg">
      <path d="M17.472 14.382c-.297-.149-1.758-.867-2.03-.967-.273-.099-.471-.148-.67.15-.197.297-.767.966-.94 1.164-.173.199-.347.223-.644.075-.297-.15-1.255-.463-2.39-1.475-.883-.788-1.48-1.761-1.653-2.059-.173-.297-.018-.458.13-.606.134-.133.298-.347.446-.52.149-.174.198-.298.298-.497.099-.198.05-.371-.025-.52-.075-.149-.669-1.612-.916-2.207-.242-.579-.487-.5-.669-.51-.173-.008-.371-.01-.57-.01-.198 0-.52.074-.792.372-.272.297-1.04 1.016-1.04 2.479 0 1.462 1.065 2.875 1.213 3.074.149.198 2.096 3.2 5.077 4.487.709.306 1.262.489 1.694.625.712.227 1.36.195 1.871.118.571-.085 1.758-.719 2.006-1.413.248-.694.248-1.289.173-1.413-.074-.124-.272-.198-.57-.347m-5.421 7.403h-.004a9.87 9.87 0 01-5.031-1.378l-.361-.214-3.741.982.998-3.648-.235-.374a9.86 9.86 0 01-1.51-5.26c.001-5.45 4.436-9.884 9.888-9.884 2.64 0 5.122 1.03 6.988 2.898a9.825 9.825 0 012.893 6.994c-.003 5.45-4.437 9.884-9.885 9.884m8.413-18.297A11.815 11.815 0 0012.05 0C5.495 0 .16 5.335.157 11.892c0 2.096.547 4.142 1.588 5.945L.057 24l6.305-1.654a11.882 11.882 0 005.683 1.448h.005c6.554 0 11.89-5.335 11.893-11.893a11.821 11.821 0 00-3.48-8.413z"/>
    </svg>
  );
}

type ActionTab = 'status' | 'note' | 'whatsapp' | 'call';

function CallIcon({ className }: { className?: string }) {
  return (
    <svg className={className} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" xmlns="http://www.w3.org/2000/svg">
      <path d="M22 16.92v3a2 2 0 01-2.18 2 19.79 19.79 0 01-8.63-3.07 19.5 19.5 0 01-6-6 19.79 19.79 0 01-3.07-8.67A2 2 0 014.11 2h3a2 2 0 012 1.72 12.84 12.84 0 00.7 2.81 2 2 0 01-.45 2.11L8.09 9.91a16 16 0 006 6l1.27-1.27a2 2 0 012.11-.45 12.84 12.84 0 002.81.7A2 2 0 0122 16.92z" />
    </svg>
  );
}

function StatusIcon({ className }: { className?: string }) {
  return (
    <svg className={className} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" xmlns="http://www.w3.org/2000/svg">
      <path d="M20.59 13.41l-7.17 7.17a2 2 0 01-2.83 0L2 12V2h10l8.59 8.59a2 2 0 010 2.82z" />
      <line x1="7" y1="7" x2="7.01" y2="7" />
    </svg>
  );
}

function NoteIcon({ className }: { className?: string }) {
  return (
    <svg className={className} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" xmlns="http://www.w3.org/2000/svg">
      <path d="M11 4H4a2 2 0 00-2 2v14a2 2 0 002 2h14a2 2 0 002-2v-7" />
      <path d="M18.5 2.5a2.121 2.121 0 013 3L12 15l-4 1 1-4 9.5-9.5z" />
    </svg>
  );
}

function friendlyError(err: any, fallback: string): string {
  const status = err.response?.status;
  const detail = err.response?.data?.detail;
  if (detail) return detail;
  switch (status) {
    case 400: return 'Invalid request. Please check your input and try again.';
    case 404: return 'Client not found in CRM. Please verify the account ID.';
    case 502: return 'CRM returned an unexpected error. Please contact an administrator.';
    case 503: return 'CRM service is temporarily unavailable. Please try again later.';
    default: return fallback;
  }
}

function ClientActionsModal({
  client,
  onClose,
}: {
  client: RetentionClient;
  onClose: () => void;
}) {
  // Active tab
  const [activeTab, setActiveTab] = useState<ActionTab>('status');

  // Current retention status (fetched on modal open)
  const [currentStatusLabel, setCurrentStatusLabel] = useState<string | null>(null);
  const [currentStatusLoading, setCurrentStatusLoading] = useState(true);

  // Fetch current retention status on modal open
  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        const res = await api.get(`/clients/${client.accountid}/crm-user`);
        if (cancelled) return;
        const statusKey = res.data?.retentionStatus;
        if (statusKey !== undefined && statusKey !== null) {
          const match = RETENTION_STATUSES.find((s) => s.key === Number(statusKey));
          setCurrentStatusLabel(match ? match.label : `Unknown (${statusKey})`);
        }
      } catch {
        // Gracefully ignore — badge simply won't show
      } finally {
        if (!cancelled) setCurrentStatusLoading(false);
      }
    })();
    return () => { cancelled = true; };
  }, [client.accountid]);

  // Retention status state
  const [selectedStatus, setSelectedStatus] = useState<string>('');
  const [statusSubmitting, setStatusSubmitting] = useState(false);
  const [statusFeedback, setStatusFeedback] = useState<{ type: 'success' | 'error'; message: string } | null>(null);

  // Add note state
  const [noteText, setNoteText] = useState('');
  const [noteSubmitting, setNoteSubmitting] = useState(false);
  const [noteFeedback, setNoteFeedback] = useState<{ type: 'success' | 'error'; message: string } | null>(null);

  // WhatsApp state
  const [waLoading, setWaLoading] = useState(false);
  const [waFeedback, setWaFeedback] = useState<{ type: 'success' | 'error'; message: string } | null>(null);

  // Call state
  const [callLoading, setCallLoading] = useState(false);
  const [callFeedback, setCallFeedback] = useState<{ type: 'success' | 'error'; message: string } | null>(null);

  const handleStatusSubmit = async () => {
    if (!selectedStatus) return;
    setStatusSubmitting(true);
    setStatusFeedback(null);
    try {
      await api.put(`/clients/${client.accountid}/retention-status`, {
        status_key: Number(selectedStatus),
      });
      const statusLabel = RETENTION_STATUSES.find((s) => s.key === Number(selectedStatus))?.label ?? selectedStatus;
      setCurrentStatusLabel(statusLabel);
      setStatusFeedback({ type: 'success', message: `Retention status updated to "${statusLabel}"` });
    } catch (err: any) {
      setStatusFeedback({ type: 'error', message: friendlyError(err, 'Failed to update retention status') });
    } finally {
      setStatusSubmitting(false);
    }
  };

  const handleNoteSubmit = async () => {
    if (!noteText.trim()) return;
    setNoteSubmitting(true);
    setNoteFeedback(null);
    try {
      await api.post(`/clients/${client.accountid}/note`, { note: noteText.trim() });
      setNoteFeedback({ type: 'success', message: 'Note added successfully' });
      setNoteText('');
    } catch (err: any) {
      setNoteFeedback({ type: 'error', message: friendlyError(err, 'Failed to add note') });
    } finally {
      setNoteSubmitting(false);
    }
  };

  const handleWhatsApp = async () => {
    setWaLoading(true);
    setWaFeedback(null);
    try {
      const res = await api.get(`/clients/${client.accountid}/crm-user`, { params: { log_whatsapp: true } });
      const phone = res.data?.fullTelephone || res.data?.telephone || res.data?.phone || res.data?.Phone || res.data?.phoneNumber || res.data?.PhoneNumber || res.data?.mobile || res.data?.Mobile;
      if (!phone) {
        setWaFeedback({ type: 'error', message: 'No phone number found for this client' });
        return;
      }
      const cleanPhone = String(phone).replace(/[^0-9+]/g, '').replace(/^\+/, '');
      const waUrl = `https://api.whatsapp.com/send?phone=${encodeURIComponent(cleanPhone)}&text=${encodeURIComponent('Hi I want to call you')}`;
      window.open(waUrl, '_blank');
      setWaFeedback({ type: 'success', message: 'WhatsApp tab opened' });
    } catch (err: any) {
      setWaFeedback({ type: 'error', message: friendlyError(err, 'Failed to fetch client phone number') });
    } finally {
      setWaLoading(false);
    }
  };

  const handleCall = async () => {
    setCallLoading(true);
    setCallFeedback(null);
    try {
      const res = await api.post(`/clients/${client.accountid}/call`);
      setCallFeedback({ type: 'success', message: res.data?.message || 'Call initiated successfully' });
    } catch (err: any) {
      setCallFeedback({ type: 'error', message: friendlyError(err, 'Failed to initiate call') });
    } finally {
      setCallLoading(false);
    }
  };

  const feedbackEl = (fb: { type: 'success' | 'error'; message: string } | null) =>
    fb ? (
      <div
        className={`px-3 py-2 rounded-md text-sm mt-3 ${
          fb.type === 'success'
            ? 'bg-green-50 text-green-700 border border-green-200'
            : 'bg-red-50 text-red-700 border border-red-200'
        }`}
      >
        {fb.message}
      </div>
    ) : null;

  const tabs: { key: ActionTab; label: string; icon: JSX.Element }[] = [
    {
      key: 'status',
      label: 'Status',
      icon: <StatusIcon className="w-5 h-5" />,
    },
    {
      key: 'note',
      label: 'Note',
      icon: <NoteIcon className="w-5 h-5" />,
    },
    {
      key: 'whatsapp',
      label: 'WhatsApp',
      icon: <WhatsAppIcon className="w-5 h-5" />,
    },
    {
      key: 'call',
      label: 'Call',
      icon: <CallIcon className="w-5 h-5" />,
    },
  ];

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40" onClick={onClose}>
      <div
        className="bg-white rounded-lg shadow-xl w-full max-w-md mx-4 max-h-[90vh] flex flex-col"
        onClick={(e) => e.stopPropagation()}
      >
        {/* Header */}
        <div className="px-6 py-4 border-b border-gray-200 flex items-center justify-between shrink-0">
          <div>
            <h2 className="text-lg font-semibold text-gray-800">Client Actions</h2>
            <p className="text-sm text-gray-500 mt-0.5">Account: {client.accountid}</p>
          </div>
          <button
            onClick={onClose}
            className="text-gray-400 hover:text-gray-600 text-xl leading-none"
            title="Close"
          >
            x
          </button>
        </div>

        {/* Current retention status badge */}
        <div className="px-6 pt-3 pb-0 shrink-0">
          {currentStatusLoading ? (
            <div className="flex items-center gap-2 text-sm text-gray-400">
              <svg className="animate-spin h-4 w-4" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
                <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
                <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z" />
              </svg>
              Loading status...
            </div>
          ) : currentStatusLabel ? (
            <div className="flex items-center gap-2 text-sm">
              <span className="text-gray-500">Current Status:</span>
              <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-blue-100 text-blue-800">
                {currentStatusLabel}
              </span>
            </div>
          ) : null}
        </div>

        {/* Icon tab navigation */}
        <div className="px-6 pt-4 pb-0 shrink-0">
          <div className="flex gap-1 justify-center">
            {tabs.map((tab) => (
              <button
                key={tab.key}
                onClick={() => {
                  setActiveTab(tab.key);
                }}
                className={`flex items-center gap-2 px-4 py-2.5 rounded-lg text-sm font-medium transition-colors ${
                  activeTab === tab.key
                    ? 'bg-blue-50 text-blue-700 border border-blue-200'
                    : 'text-gray-500 hover:text-gray-700 hover:bg-gray-50 border border-transparent'
                }`}
                title={tab.label}
              >
                {tab.icon}
                <span>{tab.label}</span>
              </button>
            ))}
          </div>
        </div>

        {/* Body — only active action shown */}
        <div className="px-6 py-5 overflow-y-auto">
          {/* Action: Change Retention Status */}
          {activeTab === 'status' && (
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-2">
                Change Retention Status
              </label>
              <select
                value={selectedStatus}
                onChange={(e) => {
                  setSelectedStatus(e.target.value);
                  setStatusFeedback(null);
                }}
                className="w-full border border-gray-300 rounded-md px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 bg-white"
                disabled={statusSubmitting}
              >
                <option value="">-- Select Status --</option>
                {RETENTION_STATUSES.map((s) => (
                  <option key={s.key} value={String(s.key)}>
                    {s.label}
                  </option>
                ))}
              </select>
              <div className="flex justify-end mt-3">
                <button
                  onClick={handleStatusSubmit}
                  disabled={!selectedStatus || statusSubmitting}
                  className="px-4 py-2 text-sm font-medium text-white bg-blue-600 rounded-md hover:bg-blue-700 disabled:opacity-40 disabled:cursor-not-allowed"
                >
                  {statusSubmitting ? 'Updating...' : 'Update Status'}
                </button>
              </div>
              {feedbackEl(statusFeedback)}
            </div>
          )}

          {/* Action: Add Note */}
          {activeTab === 'note' && (
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-2">
                Add Note
              </label>
              <textarea
                value={noteText}
                onChange={(e) => {
                  setNoteText(e.target.value);
                  setNoteFeedback(null);
                }}
                placeholder="Type a note for this client..."
                rows={4}
                className="w-full border border-gray-300 rounded-md px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 resize-none"
                disabled={noteSubmitting}
              />
              <div className="flex justify-end mt-3">
                <button
                  onClick={handleNoteSubmit}
                  disabled={!noteText.trim() || noteSubmitting}
                  className="px-4 py-2 text-sm font-medium text-white bg-blue-600 rounded-md hover:bg-blue-700 disabled:opacity-40 disabled:cursor-not-allowed"
                >
                  {noteSubmitting ? 'Submitting...' : 'Submit Note'}
                </button>
              </div>
              {feedbackEl(noteFeedback)}
            </div>
          )}

          {/* Action: Send WhatsApp */}
          {activeTab === 'whatsapp' && (
            <div className="text-center py-4">
              <p className="text-sm text-gray-600 mb-4">
                Send a WhatsApp message to this client. The phone number will be fetched from the CRM automatically.
              </p>
              <button
                onClick={handleWhatsApp}
                disabled={waLoading}
                className="inline-flex items-center gap-2 px-5 py-2.5 text-sm font-medium text-white bg-green-600 rounded-md hover:bg-green-700 disabled:opacity-40 disabled:cursor-not-allowed"
              >
                <WhatsAppIcon className="w-5 h-5" />
                {waLoading ? 'Fetching phone...' : 'Open WhatsApp'}
              </button>
              {feedbackEl(waFeedback)}
            </div>
          )}

          {/* Action: Call via SquareTalk */}
          {activeTab === 'call' && (
            <div className="text-center py-4">
              <p className="text-sm text-gray-600 mb-4">
                Initiate a phone call to this client via SquareTalk. Your extension will be looked up automatically.
              </p>
              <button
                onClick={handleCall}
                disabled={callLoading}
                className="inline-flex items-center gap-2 px-5 py-2.5 text-sm font-medium text-white bg-blue-600 rounded-md hover:bg-blue-700 disabled:opacity-40 disabled:cursor-not-allowed"
              >
                <CallIcon className="w-5 h-5" />
                {callLoading ? 'Initiating call...' : 'Call'}
              </button>
              {feedbackEl(callFeedback)}
            </div>
          )}
        </div>

        {/* Footer */}
        <div className="px-6 py-4 border-t border-gray-200 flex justify-end shrink-0">
          <button
            onClick={onClose}
            className="px-4 py-2 text-sm font-medium text-gray-600 border border-gray-300 rounded-md hover:bg-gray-50"
          >
            Close
          </button>
        </div>
      </div>
    </div>
  );
}

// ── Column definitions ─────────────────────────────────────────────────────

type ColFilterType = 'text' | 'numeric' | 'date' | 'none';

interface ColDef {
  key: string;
  label: string;
  sortKey?: SortCol;
  align?: 'left' | 'right';
  minWidth?: string;
  filterType: ColFilterType;
  filterParamKey?: string; // override the backend param prefix when it differs from col.key
  renderHeader?: (props: { colFilters: ColFilters; setColFilters: React.Dispatch<React.SetStateAction<ColFilters>>; sortBy: SortCol; sortDir: 'asc' | 'desc' }) => ReactNode;
  renderCell: (c: RetentionClient) => ReactNode;
}

const DEFAULT_COLS: ColDef[] = [
  {
    key: 'tasks',
    label: 'Tasks',
    filterType: 'none',
    renderCell: (c) => (
      c.tasks.length === 0 ? (
        <span className="text-xs text-gray-400">—</span>
      ) : (
        <div className="flex flex-wrap gap-1">
          {c.tasks.map((t) => {
            const style = TASK_COLOR_STYLES[t.color] || TASK_COLOR_STYLES.grey;
            return (
              <span key={t.name} className={`inline-block px-2 py-0.5 rounded-full text-xs font-medium ${style.bg} ${style.text} whitespace-nowrap`}>{t.name}</span>
            );
          })}
        </div>
      )
    ),
  },
  {
    key: 'score',
    label: 'Score',
    sortKey: 'score',
    align: 'right',
    minWidth: '90px',
    filterType: 'numeric',
    renderCell: (c) => <span className="text-sm text-right font-semibold text-blue-700">{c.score}</span>,
  },
  {
    key: 'agent_name',
    label: 'Agent',
    sortKey: 'agent_name',
    align: 'left',
    minWidth: '120px',
    filterType: 'text',
    filterParamKey: 'agent',
    renderCell: (c) => <span className="text-sm text-gray-700">{c.agent_name ?? '—'}</span>,
  },
  {
    key: 'sales_client_potential',
    label: 'Potential',
    sortKey: 'sales_client_potential',
    align: 'left',
    filterType: 'none',
    renderCell: (c) => <span className="text-sm text-gray-700">{c.sales_client_potential ?? '—'}</span>,
  },
  {
    key: 'age',
    label: 'Age',
    sortKey: 'age',
    align: 'left',
    filterType: 'none',
    renderCell: (c) => <span className="text-sm text-gray-700">{c.age ?? '—'}</span>,
  },
  {
    key: 'client_qualification_date',
    label: 'Qual. Date',
    sortKey: 'client_qualification_date',
    align: 'left',
    minWidth: '130px',
    filterType: 'date',
    filterParamKey: 'reg_date',
    renderCell: (c) => <span className="text-sm text-gray-700">{formatDate(c.client_qualification_date)}</span>,
  },
  {
    key: 'days_in_retention',
    label: 'Days in Ret.',
    sortKey: 'days_in_retention',
    align: 'left',
    filterType: 'none',
    renderCell: (c) => <span className="text-sm text-gray-700">{c.days_in_retention ?? '—'}</span>,
  },
  {
    key: 'trade_count',
    label: 'Trades',
    sortKey: 'trade_count',
    align: 'left',
    filterType: 'none',
    renderCell: (c) => <span className="text-sm text-gray-700">{c.trade_count.toLocaleString()}</span>,
  },
  {
    key: 'total_profit',
    label: 'Total Profit',
    sortKey: 'total_profit',
    align: 'right',
    filterType: 'none',
    renderCell: (c) => (
      <span className={`text-sm font-medium ${c.total_profit >= 0 ? 'text-green-600' : 'text-red-600'}`}>
        {fmtNum(c.total_profit)}
      </span>
    ),
  },
  {
    key: 'last_trade_date',
    label: 'Last Trade',
    sortKey: 'last_trade_date',
    align: 'left',
    minWidth: '130px',
    filterType: 'date',
    filterParamKey: 'last_call',
    renderCell: (c) => <span className="text-sm text-gray-700">{formatDate(c.last_trade_date)}</span>,
  },
  {
    key: 'days_from_last_trade',
    label: 'Days from Last Trade',
    sortKey: 'days_from_last_trade',
    align: 'left',
    filterType: 'none',
    renderCell: (c) => <span className="text-sm text-gray-700">{c.days_from_last_trade ?? '—'}</span>,
  },
  {
    key: 'deposit_count',
    label: 'Deposits',
    sortKey: 'deposit_count',
    align: 'left',
    filterType: 'none',
    renderCell: (c) => <span className="text-sm text-gray-700">{c.deposit_count.toLocaleString()}</span>,
  },
  {
    key: 'total_deposit',
    label: 'Total Deposit',
    sortKey: 'total_deposit',
    align: 'right',
    filterType: 'none',
    renderCell: (c) => <span className="text-sm text-right text-gray-700">{fmtNum(c.total_deposit)}</span>,
  },
  {
    key: 'balance',
    label: 'Balance',
    sortKey: 'balance',
    align: 'right',
    minWidth: '110px',
    filterType: 'numeric',
    renderCell: (c) => <span className="text-sm text-right text-gray-700">{fmtNum(c.balance)}</span>,
  },
  {
    key: 'credit',
    label: 'Credit',
    sortKey: 'credit',
    align: 'right',
    minWidth: '110px',
    filterType: 'numeric',
    renderCell: (c) => <span className="text-sm text-right text-gray-700">{fmtNum(c.credit)}</span>,
  },
  {
    key: 'equity',
    label: 'Equity',
    sortKey: 'equity',
    align: 'right',
    minWidth: '110px',
    filterType: 'numeric',
    renderCell: (c) => <span className="text-sm text-right text-gray-700">{fmtNum(c.equity)}</span>,
  },
  {
    key: 'open_pnl',
    label: 'Open PNL',
    sortKey: 'open_pnl',
    align: 'right',
    filterType: 'none',
    renderCell: (c) => (
      <span className={`text-sm font-medium ${c.open_pnl >= 0 ? 'text-green-600' : 'text-red-600'}`}>
        {fmtNum(c.open_pnl)}
      </span>
    ),
  },
  {
    key: 'live_equity',
    label: 'Live Equity',
    sortKey: 'live_equity',
    align: 'right',
    minWidth: '110px',
    filterType: 'numeric',
    renderCell: (c) => (
      <span className={`text-sm font-medium ${c.live_equity >= 0 ? 'text-green-600' : 'text-red-600'}`}>
        {fmtNum(c.live_equity)}
      </span>
    ),
  },
  {
    key: 'max_open_trade',
    label: 'Max Open Trade',
    sortKey: 'max_open_trade',
    align: 'right',
    minWidth: '120px',
    filterType: 'numeric',
    renderCell: (c) => <span className="text-sm text-right text-gray-700">{c.max_open_trade != null ? fmtNum(c.max_open_trade, 1) : '\u2014'}</span>,
  },
  {
    key: 'max_volume',
    label: 'Max Volume',
    sortKey: 'max_volume',
    align: 'right',
    minWidth: '110px',
    filterType: 'numeric',
    renderCell: (c) => <span className="text-sm text-right text-gray-700">{c.max_volume != null ? fmtNum(c.max_volume, 1) : '\u2014'}</span>,
  },
  {
    key: 'win_rate',
    label: 'Win Rate',
    sortKey: 'win_rate',
    align: 'right',
    minWidth: '100px',
    filterType: 'none',
    renderCell: (c) => (
      <span className={`text-sm font-medium ${c.win_rate == null ? 'text-gray-400' : c.win_rate >= 50 ? 'text-green-600' : 'text-red-500'}`}>
        {c.win_rate != null ? `${c.win_rate.toFixed(1)}%` : '—'}
      </span>
    ),
  },
  {
    key: 'avg_trade_size',
    label: 'Avg Trade Size',
    sortKey: 'avg_trade_size',
    align: 'right',
    minWidth: '120px',
    filterType: 'none',
    renderCell: (c) => <span className="text-sm text-gray-700">{c.avg_trade_size != null ? fmtNum(c.avg_trade_size) : '—'}</span>,
  },
  {
    key: 'turnover',
    label: 'Turnover',
    sortKey: 'turnover',
    align: 'right',
    minWidth: '110px',
    filterType: 'numeric',
    renderCell: (c) => <span className="text-sm text-right text-gray-700">{fmtNum(c.turnover, 1)}</span>,
  },
  {
    key: 'active',
    label: 'Active',
    sortKey: 'active',
    align: 'left',
    filterType: 'none',
    renderCell: (c) => <BoolBadge value={c.active} />,
  },
  {
    key: 'active_ftd',
    label: 'Active FTD',
    sortKey: 'active_ftd',
    align: 'left',
    filterType: 'none',
    renderCell: (c) => <BoolBadge value={c.active_ftd} />,
  },
];

// Keys of the draggable columns (excludes pinned: accountid, full_name)
const DEFAULT_COL_ORDER = DEFAULT_COLS.map((c) => c.key);

// Lookup map for quick access
const COL_DEF_MAP = Object.fromEntries(DEFAULT_COLS.map((c) => [c.key, c]));

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
  const [agents, setAgents] = useState<{ id: string; name: string }[]>([]);
  const [taskList, setTaskList] = useState<{ id: number; name: string }[]>([]);
  const [selectedClient, setSelectedClient] = useState<RetentionClient | null>(null);
  const [colFilters, setColFilters] = useState<ColFilters>({});
  // Debounced colFilters that actually trigger the API call
  const [debouncedColFilters, setDebouncedColFilters] = useState<ColFilters>({});
  const colFiltersDebounceRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  // ── Column order state ─────────────────────────────────────────────────
  const [colOrder, setColOrder] = useState<string[]>(DEFAULT_COL_ORDER);
  const [colOrderLoaded, setColOrderLoaded] = useState(false);
  // Drag state — track which column key is being dragged and which is the drop target
  const dragColRef = useRef<string | null>(null);
  const [dragOverCol, setDragOverCol] = useState<string | null>(null);

  // On mount: fetch saved column order from server
  useEffect(() => {
    api.get('/preferences/columns')
      .then((res) => {
        const saved: string[] | null = res.data?.order ?? null;
        if (Array.isArray(saved) && saved.length > 0) {
          // Merge: saved order first, then append any new columns not in saved order
          const validSaved = saved.filter((k) => COL_DEF_MAP[k]);
          const missing = DEFAULT_COL_ORDER.filter((k) => !validSaved.includes(k));
          setColOrder([...validSaved, ...missing]);
        }
      })
      .catch(() => {
        // Silently fall back to default order
      })
      .finally(() => setColOrderLoaded(true));
  }, []);

  // Persist column order to server (immediate after drop)
  const saveColOrder = useCallback((order: string[]) => {
    api.put('/preferences/columns', { order }).catch(() => {
      // Silently ignore persistence failures
    });
  }, []);

  // ── Drag & drop handlers ───────────────────────────────────────────────
  const handleDragStart = useCallback((key: string) => {
    dragColRef.current = key;
  }, []);

  const handleDragOver = useCallback((e: React.DragEvent, key: string) => {
    e.preventDefault(); // required to allow drop
    setDragOverCol(key);
  }, []);

  const handleDragLeave = useCallback(() => {
    setDragOverCol(null);
  }, []);

  const handleDrop = useCallback((e: React.DragEvent, targetKey: string) => {
    e.preventDefault();
    setDragOverCol(null);
    const sourceKey = dragColRef.current;
    if (!sourceKey || sourceKey === targetKey) return;
    setColOrder((prev) => {
      const next = [...prev];
      const fromIdx = next.indexOf(sourceKey);
      const toIdx = next.indexOf(targetKey);
      if (fromIdx === -1 || toIdx === -1) return prev;
      next.splice(fromIdx, 1);
      next.splice(toIdx, 0, sourceKey);
      saveColOrder(next);
      return next;
    });
    dragColRef.current = null;
  }, [saveColOrder]);

  const handleDragEnd = useCallback(() => {
    dragColRef.current = null;
    setDragOverCol(null);
  }, []);

  // Reset column order to default
  const resetColOrder = useCallback(() => {
    setColOrder(DEFAULT_COL_ORDER);
    saveColOrder(DEFAULT_COL_ORDER);
  }, [saveColOrder]);

  // Ordered column definitions (draggable columns only; pinned prepended in render)
  const orderedCols = useMemo(
    () => colOrder.map((k) => COL_DEF_MAP[k]).filter(Boolean) as ColDef[],
    [colOrder],
  );

  // Total column count: 2 pinned + ordered draggable cols
  const totalColCount = 2 + orderedCols.length;

  useEffect(() => {
    api.get('/retention/agents').then((r) => setAgents(r.data)).catch(() => {});
    api.get('/retention/tasks').then((r) => setTaskList(r.data)).catch(() => {});
  }, []);

  const load = async (p: number, col: SortCol, dir: 'asc' | 'desc', f: Filters, actDays: string, cf: ColFilters) => {
    setLoading(true);
    setError('');

    // Build col-filter params
    const colFilterParams: Record<string, string> = {};
    for (const [key, filter] of Object.entries(cf)) {
      if (!filter) continue;
      // Use filterParamKey override if defined (handles mismatches like agent_name→agent)
      const paramKey = COL_DEF_MAP[key]?.filterParamKey || key;
      if (filter.type === 'text' && filter.value) {
        colFilterParams[`filter_${paramKey}`] = filter.value;
      } else if (filter.type === 'numeric' && filter.val) {
        colFilterParams[`filter_${paramKey}_op`] = filter.op;
        colFilterParams[`filter_${paramKey}_val`] = filter.val;
        if (filter.op === 'between' && filter.val2) {
          colFilterParams[`filter_${paramKey}_val2`] = filter.val2;
        }
      } else if (filter.type === 'date') {
        if (filter.preset && filter.preset !== 'custom') {
          colFilterParams[`filter_${paramKey}_preset`] = filter.preset;
        } else if (filter.from) {
          colFilterParams[`filter_${paramKey}_from`] = filter.from;
          if (filter.to) colFilterParams[`filter_${paramKey}_to`] = filter.to;
        }
      }
    }

    try {
      const res = await api.get('/retention/clients', {
        params: {
          page: p, page_size: PAGE_SIZE, sort_by: col, sort_dir: dir,
          accountid: f.accountid,
          ...colFilterParams,
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
          equity_op: f.equity_op, equity_val: f.equity_val || undefined,
          live_equity_op: f.live_equity_op, live_equity_val: f.live_equity_val || undefined,
          max_open_trade_op: f.max_open_trade_op, max_open_trade_val: f.max_open_trade_val || undefined,
          max_volume_op: f.max_volume_op, max_volume_val: f.max_volume_val || undefined,
          turnover_op: f.turnover_op, turnover_val: f.turnover_val || undefined,
          assigned_to: f.assigned_to || undefined,
          task_id: f.task_id || undefined,
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

  // Debounce colFilters changes → update debouncedColFilters after 400ms
  useEffect(() => {
    if (colFiltersDebounceRef.current) clearTimeout(colFiltersDebounceRef.current);
    colFiltersDebounceRef.current = setTimeout(() => {
      setDebouncedColFilters(colFilters);
      setPage(1);
    }, 400);
    return () => {
      if (colFiltersDebounceRef.current) clearTimeout(colFiltersDebounceRef.current);
    };
  }, [colFilters]);

  useEffect(() => { load(page, sortBy, sortDir, applied, activityDays, debouncedColFilters); }, [page, sortBy, sortDir, applied, activityDays, debouncedColFilters]);

  const handleSort = (col: SortCol) => {
    if (sortBy === col) setSortDir((d) => (d === 'asc' ? 'desc' : 'asc'));
    else { setSortBy(col); setSortDir('asc'); }
    setPage(1);
  };

  const applyFilters = () => { setApplied({ ...draft }); setPage(1); setActivityDays(activityDays); };
  const clearFilters = () => { setDraft(EMPTY_FILTERS); setApplied(EMPTY_FILTERS); setPage(1); };
  const clearColFilters = () => { setColFilters({}); };
  const setField = <K extends keyof Filters>(key: K, val: Filters[K]) => setDraft((prev) => ({ ...prev, [key]: val }));

  const activeColFilterCount = useMemo(() => Object.keys(colFilters).length, [colFilters]);

  const totalPages = data ? Math.ceil(data.total / PAGE_SIZE) : 0;
  const activeCount = countActive(applied);

  // ── Active filter chips ─────────────────────────────────────────────────
  const filterChips = useMemo(() => {
    const chips: { label: string; key: string; onDismiss: () => void }[] = [];
    const opLabel = (op: string) => ({ eq: '=', gt: '>', lt: '<', gte: '≥', lte: '≤' }[op] ?? op);
    const fmtD = (d: string) => d ? new Date(d + 'T00:00:00').toLocaleDateString('en-GB', { day: '2-digit', month: '2-digit', year: 'numeric' }) : '';
    const presetLabel: Record<string, string> = { today: 'Today', this_week: 'This Week', this_month: 'This Month' };
    const dismiss = (fields: Partial<Filters>) => () => {
      setApplied((prev) => ({ ...prev, ...fields }));
      setDraft((prev) => ({ ...prev, ...fields }));
      setPage(1);
    };

    if (applied.accountid) chips.push({ key: 'accountid', label: `Account ID contains "${applied.accountid}"`, onDismiss: dismiss({ accountid: '' }) });
    if (applied.qual_date_from || applied.qual_date_to) chips.push({ key: 'qual_date', label: `Qual Date: ${fmtD(applied.qual_date_from)} – ${fmtD(applied.qual_date_to)}`, onDismiss: dismiss({ qual_date_from: '', qual_date_to: '' }) });
    if (applied.trade_count_op && applied.trade_count_val) chips.push({ key: 'trade_count', label: `Trades ${opLabel(applied.trade_count_op)} ${applied.trade_count_val}`, onDismiss: dismiss({ trade_count_op: '', trade_count_val: '' }) });
    if (applied.days_op && applied.days_val) chips.push({ key: 'days', label: `Days in Retention ${opLabel(applied.days_op)} ${applied.days_val}`, onDismiss: dismiss({ days_op: '', days_val: '' }) });
    if (applied.profit_op && applied.profit_val) chips.push({ key: 'profit', label: `Total Profit ${opLabel(applied.profit_op)} ${applied.profit_val}`, onDismiss: dismiss({ profit_op: '', profit_val: '' }) });
    if (applied.last_trade_preset && applied.last_trade_preset !== 'custom')
      chips.push({ key: 'last_trade', label: `Last Trade: ${presetLabel[applied.last_trade_preset] ?? applied.last_trade_preset}`, onDismiss: dismiss({ last_trade_preset: '', last_trade_from: '', last_trade_to: '' }) });
    else if (applied.last_trade_from || applied.last_trade_to)
      chips.push({ key: 'last_trade', label: `Last Trade: ${fmtD(applied.last_trade_from)} – ${fmtD(applied.last_trade_to)}`, onDismiss: dismiss({ last_trade_preset: '', last_trade_from: '', last_trade_to: '' }) });
    if (applied.days_from_last_trade_op && applied.days_from_last_trade_val) chips.push({ key: 'days_from_last_trade', label: `Days from Last Trade ${opLabel(applied.days_from_last_trade_op)} ${applied.days_from_last_trade_val}`, onDismiss: dismiss({ days_from_last_trade_op: '', days_from_last_trade_val: '' }) });
    if (applied.deposit_count_op && applied.deposit_count_val) chips.push({ key: 'deposit_count', label: `Deposits ${opLabel(applied.deposit_count_op)} ${applied.deposit_count_val}`, onDismiss: dismiss({ deposit_count_op: '', deposit_count_val: '' }) });
    if (applied.total_deposit_op && applied.total_deposit_val) chips.push({ key: 'total_deposit', label: `Total Deposit ${opLabel(applied.total_deposit_op)} ${applied.total_deposit_val}`, onDismiss: dismiss({ total_deposit_op: '', total_deposit_val: '' }) });
    if (applied.balance_op && applied.balance_val) chips.push({ key: 'balance', label: `Balance ${opLabel(applied.balance_op)} ${applied.balance_val}`, onDismiss: dismiss({ balance_op: '', balance_val: '' }) });
    if (applied.credit_op && applied.credit_val) chips.push({ key: 'credit', label: `Credit ${opLabel(applied.credit_op)} ${applied.credit_val}`, onDismiss: dismiss({ credit_op: '', credit_val: '' }) });
    if (applied.equity_op && applied.equity_val) chips.push({ key: 'equity', label: `Equity ${opLabel(applied.equity_op)} ${applied.equity_val}`, onDismiss: dismiss({ equity_op: '', equity_val: '' }) });
    if (applied.live_equity_op && applied.live_equity_val) chips.push({ key: 'live_equity', label: `Live Equity ${opLabel(applied.live_equity_op)} ${applied.live_equity_val}`, onDismiss: dismiss({ live_equity_op: '', live_equity_val: '' }) });
    if (applied.max_open_trade_op && applied.max_open_trade_val) chips.push({ key: 'max_open_trade', label: `Max Open Trade ${opLabel(applied.max_open_trade_op)} ${applied.max_open_trade_val}`, onDismiss: dismiss({ max_open_trade_op: '', max_open_trade_val: '' }) });
    if (applied.max_volume_op && applied.max_volume_val) chips.push({ key: 'max_volume', label: `Max Volume ${opLabel(applied.max_volume_op)} ${applied.max_volume_val}`, onDismiss: dismiss({ max_volume_op: '', max_volume_val: '' }) });
    if (applied.turnover_op && applied.turnover_val) chips.push({ key: 'turnover', label: `Turnover ${opLabel(applied.turnover_op)} ${applied.turnover_val}`, onDismiss: dismiss({ turnover_op: '', turnover_val: '' }) });
    if (applied.assigned_to) {
      const agentName = agents.find((a) => a.id === applied.assigned_to)?.name ?? applied.assigned_to;
      chips.push({ key: 'assigned_to', label: `Agent: ${agentName}`, onDismiss: dismiss({ assigned_to: '' }) });
    }
    if (applied.task_id) {
      const taskName = taskList.find((t) => String(t.id) === applied.task_id)?.name ?? `Task #${applied.task_id}`;
      chips.push({ key: 'task_id', label: `Task: ${taskName}`, onDismiss: dismiss({ task_id: '' }) });
    }
    if (applied.active) chips.push({ key: 'active', label: `Active: ${applied.active === 'true' ? 'Yes' : 'No'}`, onDismiss: dismiss({ active: '' }) });
    if (applied.active_ftd) chips.push({ key: 'active_ftd', label: `Active FTD: ${applied.active_ftd === 'true' ? 'Yes' : 'No'}`, onDismiss: dismiss({ active_ftd: '' }) });

    // Column header filters
    for (const [colKey, cf] of Object.entries(colFilters)) {
      if (!cf) continue;
      const colLabel = COL_DEF_MAP[colKey]?.label ?? colKey;
      const dismissCol = () => setColFilters((prev) => { const next = { ...prev }; delete next[colKey]; return next; });
      if (cf.type === 'text' && cf.value) {
        chips.push({ key: `col_${colKey}`, label: `${colLabel} contains "${cf.value}"`, onDismiss: dismissCol });
      } else if (cf.type === 'numeric' && cf.val) {
        const lbl = cf.op === 'between' ? `${colLabel} between ${cf.val} – ${cf.val2 ?? ''}` : `${colLabel} ${opLabel(cf.op)} ${cf.val}`;
        chips.push({ key: `col_${colKey}`, label: lbl, onDismiss: dismissCol });
      } else if (cf.type === 'date') {
        if (cf.preset && cf.preset !== 'custom') {
          chips.push({ key: `col_${colKey}`, label: `${colLabel}: ${presetLabel[cf.preset] ?? cf.preset}`, onDismiss: dismissCol });
        } else if (cf.from) {
          const rangeLabel = cf.to ? `${fmtD(cf.from)} – ${fmtD(cf.to)}` : `from ${fmtD(cf.from)}`;
          chips.push({ key: `col_${colKey}`, label: `${colLabel}: ${rangeLabel}`, onDismiss: dismissCol });
        }
      }
    }
    return chips;
  }, [applied, colFilters, agents, taskList]); // eslint-disable-line react-hooks/exhaustive-deps

  // Check if order differs from default
  const isCustomOrder = useMemo(
    () => colOrder.join(',') !== DEFAULT_COL_ORDER.join(','),
    [colOrder],
  );

  // ── Virtual scrolling setup ──
  const ROW_HEIGHT = 44; // estimated row height in px
  const scrollContainerRef = useRef<HTMLDivElement>(null);
  const clients = data?.clients ?? [];

  const rowVirtualizer = useVirtualizer({
    count: clients.length,
    getScrollElement: () => scrollContainerRef.current,
    estimateSize: useCallback(() => ROW_HEIGHT, []),
    overscan: 10,
  });

  // Render a column header <th>
  const renderColHeader = (col: ColDef) => {
    const isDragOver = dragOverCol === col.key;
    const baseClass = [
      'px-4 pt-3 pb-1 text-xs font-medium text-gray-500 uppercase tracking-wider whitespace-nowrap align-top select-none',
      col.sortKey ? 'cursor-pointer hover:bg-gray-100' : '',
      col.align === 'right' ? 'text-right' : 'text-left',
      col.minWidth ? `min-w-[${col.minWidth}]` : '',
      // Drag-over highlight
      isDragOver ? 'border-l-2 border-blue-500 bg-blue-50' : '',
    ].filter(Boolean).join(' ');

    return (
      <th
        key={col.key}
        className={baseClass}
        draggable
        onDragStart={() => handleDragStart(col.key)}
        onDragOver={(e) => handleDragOver(e, col.key)}
        onDragLeave={handleDragLeave}
        onDrop={(e) => handleDrop(e, col.key)}
        onDragEnd={handleDragEnd}
        onClick={col.sortKey ? () => handleSort(col.sortKey!) : undefined}
        title="Drag to reorder"
        style={col.minWidth ? { minWidth: col.minWidth } : undefined}
      >
        <span className="flex items-center gap-0.5 cursor-grab active:cursor-grabbing">
          <span className="text-gray-300 text-xs mr-0.5" aria-hidden>⠿</span>
          {col.label}
          {col.sortKey && <SortIcon col={col.sortKey} sortBy={sortBy} sortDir={sortDir} />}
        </span>
        {col.filterType === 'text' && (
          <ColTextFilter col={col.key} colFilters={colFilters} setColFilters={setColFilters} />
        )}
        {col.filterType === 'numeric' && (
          <ColNumericFilter col={col.key} colFilters={colFilters} setColFilters={setColFilters} />
        )}
        {col.filterType === 'date' && (
          <ColDateFilter col={col.key} colFilters={colFilters} setColFilters={setColFilters} />
        )}
      </th>
    );
  };

  // Don't render until column order is loaded to avoid flicker
  if (!colOrderLoaded) {
    return (
      <div className="flex items-center justify-center py-20 text-sm text-gray-400">
        Loading...
      </div>
    );
  }

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
              <NumericFilter label="Equity" op={draft.equity_op} val={draft.equity_val}
                onOp={(v) => setField('equity_op', v)} onVal={(v) => setField('equity_val', v)} />
              <NumericFilter label="Live Equity" op={draft.live_equity_op} val={draft.live_equity_val}
                onOp={(v) => setField('live_equity_op', v)} onVal={(v) => setField('live_equity_val', v)} />
              <NumericFilter label="Max Open Trade" op={draft.max_open_trade_op} val={draft.max_open_trade_val}
                onOp={(v) => setField('max_open_trade_op', v)} onVal={(v) => setField('max_open_trade_val', v)} />
              <NumericFilter label="Max Volume" op={draft.max_volume_op} val={draft.max_volume_val}
                onOp={(v) => setField('max_volume_op', v)} onVal={(v) => setField('max_volume_val', v)} />
              <NumericFilter label="Turnover" op={draft.turnover_op} val={draft.turnover_val}
                onOp={(v) => setField('turnover_op', v)} onVal={(v) => setField('turnover_val', v)} />
              {/* Agent filter */}
              <div>
                <label className="block text-xs font-medium text-gray-600 mb-1">Agent</label>
                <select
                  value={draft.assigned_to}
                  onChange={(e) => setField('assigned_to', e.target.value)}
                  className="border border-gray-300 rounded-md px-2 py-1.5 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 bg-white w-full"
                >
                  <option value="">All agents</option>
                  {agents.map((a) => (
                    <option key={a.id} value={a.id}>{a.name || a.id}</option>
                  ))}
                </select>
              </div>
              {/* Task filter */}
              <div>
                <label className="block text-xs font-medium text-gray-600 mb-1">Task</label>
                <select
                  value={draft.task_id}
                  onChange={(e) => setField('task_id', e.target.value)}
                  className="border border-gray-300 rounded-md px-2 py-1.5 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 bg-white w-full"
                >
                  <option value="">All clients</option>
                  {taskList.map((t) => (
                    <option key={t.id} value={String(t.id)}>{t.name}</option>
                  ))}
                </select>
              </div>
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

      {/* Active filter chips bar */}
      {filterChips.length > 0 && (
        <div className="flex flex-wrap gap-2 items-center px-3 py-2.5 bg-blue-50 border border-blue-100 rounded-lg">
          {filterChips.map((chip) => (
            <span key={chip.key} className="inline-flex items-center gap-1 px-2.5 py-1 bg-white border border-blue-200 text-blue-800 text-xs font-medium rounded-full shadow-sm">
              {chip.label}
              <button onClick={chip.onDismiss} className="ml-0.5 text-blue-400 hover:text-blue-700 leading-none text-sm font-bold" aria-label={`Remove filter: ${chip.label}`}>×</button>
            </span>
          ))}
          <button onClick={() => { clearFilters(); clearColFilters(); }} className="ml-auto text-xs text-blue-600 hover:text-blue-800 font-medium underline whitespace-nowrap">
            Clear all
          </button>
        </div>
      )}

      {/* Table */}
      <div className="bg-white rounded-lg shadow overflow-hidden">
        <div className="px-4 py-3 border-b border-gray-200 bg-gray-50 flex items-center justify-between gap-2 flex-wrap">
          <div className="flex items-center gap-2">
            <span className="text-sm text-gray-600">
              {loading ? 'Loading…' : `${data?.total?.toLocaleString() ?? 0} accounts${activeCount > 0 ? ' (filtered)' : ''} — showing ${((page - 1) * PAGE_SIZE) + 1}–${Math.min(page * PAGE_SIZE, data?.total ?? 0)}`}
            </span>
            {activeColFilterCount > 0 && (
              <button
                onClick={clearColFilters}
                className="inline-flex items-center gap-1 px-2.5 py-1 text-xs font-medium rounded-md border border-orange-300 text-orange-700 bg-orange-50 hover:bg-orange-100"
                title="Clear all column filters"
              >
                Clear Column Filters ({activeColFilterCount})
              </button>
            )}
            {isCustomOrder && (
              <button
                onClick={resetColOrder}
                className="inline-flex items-center gap-1 px-2.5 py-1 text-xs font-medium rounded-md border border-gray-300 text-gray-600 bg-white hover:bg-gray-50"
                title="Reset column order to default"
              >
                Reset Columns
              </button>
            )}
          </div>
          {totalPages > 1 && (
            <div className="flex items-center gap-2">
              <button onClick={() => setPage((p) => Math.max(1, p - 1))} disabled={page === 1 || loading} className="px-3 py-1 text-xs rounded-md border border-gray-300 disabled:opacity-40 hover:bg-gray-50">← Prev</button>
              <span className="text-xs text-gray-500">{page} / {totalPages}</span>
              <button onClick={() => setPage((p) => Math.min(totalPages, p + 1))} disabled={page === totalPages || loading} className="px-3 py-1 text-xs rounded-md border border-gray-300 disabled:opacity-40 hover:bg-gray-50">Next →</button>
            </div>
          )}
        </div>

        {error && <div className="px-4 py-3 bg-red-50 border-b border-red-100 text-sm text-red-600">{error}</div>}

        {/* Scroll container: persistent horizontal scrollbar + fixed height for virtual scrolling + sticky header */}
        <div
          ref={scrollContainerRef}
          className="overflow-x-scroll overflow-y-auto"
          style={{ maxHeight: 'calc(100vh - 260px)' }}
        >
          <table className="w-full">
            <thead className="bg-gray-50 sticky top-0 z-10 shadow-[0_1px_0_0_rgba(229,231,235,1)]">
              <tr>
                {/* Pinned: Account ID — not draggable */}
                <th className="px-4 pt-3 pb-1 text-left text-xs font-medium text-gray-500 uppercase tracking-wider cursor-pointer select-none hover:bg-gray-100 whitespace-nowrap align-top min-w-[120px]" onClick={() => handleSort('accountid')}>
                  <span>Account ID <SortIcon col="accountid" sortBy={sortBy} sortDir={sortDir} /></span>
                  <ColTextFilter col="accountid" colFilters={colFilters} setColFilters={setColFilters} />
                </th>
                {/* Pinned: Full Name — not draggable */}
                <th className="px-4 pt-3 pb-1 text-left text-xs font-medium text-gray-500 uppercase tracking-wider cursor-pointer select-none hover:bg-gray-100 whitespace-nowrap align-top min-w-[140px]" onClick={() => handleSort('full_name')}>
                  <span>Full Name <SortIcon col="full_name" sortBy={sortBy} sortDir={sortDir} /></span>
                  <ColTextFilter col="full_name" colFilters={colFilters} setColFilters={setColFilters} />
                </th>
                {/* Draggable columns in current order */}
                {orderedCols.map((col) => renderColHeader(col))}
              </tr>
            </thead>
            <tbody>
              {loading ? (
                <tr><td colSpan={totalColCount} className="px-4 py-12 text-center text-sm text-gray-400">Loading…</td></tr>
              ) : !data || clients.length === 0 ? (
                <tr><td colSpan={totalColCount} className="px-4 py-12 text-center text-sm text-gray-400">No accounts found.</td></tr>
              ) : (
                <>
                  {/* Spacer for virtual scroll — pushes visible rows to correct offset */}
                  {rowVirtualizer.getVirtualItems().length > 0 && (
                    <tr style={{ height: rowVirtualizer.getVirtualItems()[0].start }}>
                      <td colSpan={totalColCount} style={{ padding: 0, border: 'none' }} />
                    </tr>
                  )}
                  {rowVirtualizer.getVirtualItems().map((virtualRow) => {
                    const c = clients[virtualRow.index];
                    return (
                      <tr
                        key={c.accountid}
                        data-index={virtualRow.index}
                        ref={rowVirtualizer.measureElement}
                        className="border-t border-gray-100 hover:bg-gray-50 cursor-pointer"
                        onDoubleClick={() => setSelectedClient(c)}
                      >
                        {/* Pinned cells */}
                        <td className="px-4 py-3 text-sm font-medium">
                          <a href={`https://crm.cmtrading.com/#/users/user/${c.accountid}`} target="_blank" rel="noreferrer" className="text-blue-600 hover:underline">{c.accountid}</a>
                        </td>
                        <td className="px-4 py-3 text-sm text-gray-700 whitespace-nowrap">{c.full_name || '\u2014'}</td>
                        {/* Ordered draggable cells */}
                        {orderedCols.map((col) => (
                          <td
                            key={col.key}
                            className={`px-4 py-3 ${col.align === 'right' ? 'text-right' : ''}`}
                          >
                            {col.renderCell(c)}
                          </td>
                        ))}
                      </tr>
                    );
                  })}
                  {/* Bottom spacer for virtual scroll */}
                  {rowVirtualizer.getVirtualItems().length > 0 && (
                    <tr style={{ height: rowVirtualizer.getTotalSize() - (rowVirtualizer.getVirtualItems()[rowVirtualizer.getVirtualItems().length - 1].end) }}>
                      <td colSpan={totalColCount} style={{ padding: 0, border: 'none' }} />
                    </tr>
                  )}
                </>
              )}
            </tbody>
          </table>
        </div>
      </div>

      {/* Client Actions Modal (triggered by double-click) */}
      {selectedClient && (
        <ClientActionsModal
          client={selectedClient}
          onClose={() => setSelectedClient(null)}
        />
      )}
    </div>
  );
}
