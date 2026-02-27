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
type SortCol = 'accountid' | 'client_qualification_date' | 'days_in_retention' | 'trade_count' | 'total_profit' | 'last_trade_date' | 'days_from_last_trade' | 'active' | 'active_ftd' | 'deposit_count' | 'total_deposit' | 'balance' | 'credit' | 'equity' | 'open_pnl' | 'sales_client_potential' | 'age' | 'agent_name' | 'score';
type NumOp = '' | 'eq' | 'gt' | 'gte' | 'lt' | 'lte';
type BoolFilter = '' | 'true' | 'false';

interface TaskInfo {
  name: string;
  color: string;
}

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
    f.assigned_to,
    f.task_id,
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
      const res = await api.get(`/clients/${client.accountid}/crm-user`);
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

  useEffect(() => {
    api.get('/retention/agents').then((r) => setAgents(r.data)).catch(() => {});
    api.get('/retention/tasks').then((r) => setTaskList(r.data)).catch(() => {});
  }, []);

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
          equity_op: f.equity_op, equity_val: f.equity_val || undefined,
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
              <NumericFilter label="Equity" op={draft.equity_op} val={draft.equity_val}
                onOp={(v) => setField('equity_op', v)} onVal={(v) => setField('equity_val', v)} />
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
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider whitespace-nowrap">Tasks</th>
                <th className={thClassRight} onClick={() => handleSort('score')}>Score <SortIcon col="score" sortBy={sortBy} sortDir={sortDir} /></th>
                <th className={thClass} onClick={() => handleSort('agent_name')}>Agent <SortIcon col="agent_name" sortBy={sortBy} sortDir={sortDir} /></th>
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
                <tr><td colSpan={20} className="px-4 py-12 text-center text-sm text-gray-400">Loading…</td></tr>
              ) : !data || data.clients.length === 0 ? (
                <tr><td colSpan={20} className="px-4 py-12 text-center text-sm text-gray-400">No accounts found.</td></tr>
              ) : (
                data.clients.map((c) => (
                  <tr key={c.accountid} className="border-t border-gray-100 hover:bg-gray-50 cursor-pointer" onDoubleClick={() => setSelectedClient(c)}>
                    <td className="px-4 py-3 text-sm font-medium">
                      <a href={`https://crm.cmtrading.com/#/users/user/${c.accountid}`} target="_blank" rel="noreferrer" className="text-blue-600 hover:underline">{c.accountid}</a>
                    </td>
                    <td className="px-4 py-3">
                      {c.tasks.length === 0 ? (
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
                      )}
                    </td>
                    <td className="px-4 py-3 text-sm text-right font-semibold text-blue-700">{c.score}</td>
                    <td className="px-4 py-3 text-sm text-gray-700">{c.agent_name ?? '—'}</td>
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
