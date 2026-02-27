import { useEffect, useState } from 'react';
import axios from 'axios';

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

interface ScoringRule {
  id: number;
  field: string;
  operator: string;
  value: string;
  score: number;
  created_at: string;
}

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const FIELD_OPTIONS = [
  { key: 'balance',              label: 'Balance' },
  { key: 'credit',               label: 'Credit' },
  { key: 'equity',               label: 'Equity' },
  { key: 'trade_count',          label: 'Trade Count' },
  { key: 'total_profit',         label: 'Total Profit' },
  { key: 'days_in_retention',    label: 'Days in Retention' },
  { key: 'deposit_count',        label: 'Deposit Count' },
  { key: 'total_deposit',        label: 'Total Deposit' },
  { key: 'days_from_last_trade', label: 'Days from Last Trade' },
  { key: 'sales_potential',      label: 'Sales Potential' },
  { key: 'age',                  label: 'Age' },
  { key: 'live_equity',          label: 'Live Equity' },
  { key: 'max_open_trade',       label: 'Max Open Trade' },
  { key: 'max_volume',           label: 'Max Volume' },
  { key: 'turnover',             label: 'Turnover' },
];

const OP_OPTIONS = [
  { key: 'eq',  label: '= Equal' },
  { key: 'gt',  label: '> Greater than' },
  { key: 'lt',  label: '< Less than' },
  { key: 'gte', label: '>= At least' },
  { key: 'lte', label: '<= At most' },
];

const OP_LABELS: Record<string, string> = { eq: '=', gt: '>', lt: '<', gte: '>=', lte: '<=' };

const FIELD_LABELS: Record<string, string> = Object.fromEntries(
  FIELD_OPTIONS.map((f) => [f.key, f.label])
);

// ---------------------------------------------------------------------------
// Main component
// ---------------------------------------------------------------------------

export function ClientScoringPage() {
  const [rules, setRules] = useState<ScoringRule[]>([]);
  const [loading, setLoading] = useState(true);

  // Form state
  const [formOpen, setFormOpen] = useState(false);
  const [editingRule, setEditingRule] = useState<ScoringRule | null>(null);
  const [formField, setFormField] = useState('balance');
  const [formOp, setFormOp] = useState('gt');
  const [formValue, setFormValue] = useState('');
  const [formScore, setFormScore] = useState('');
  const [formError, setFormError] = useState('');
  const [formSaving, setFormSaving] = useState(false);

  // ---------------------------------------------------------------------------
  // Data loading
  // ---------------------------------------------------------------------------

  async function loadRules() {
    try {
      const res = await api.get<ScoringRule[]>('/retention/scoring-rules');
      setRules(res.data);
    } catch {
      // silently ignore on first load
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    loadRules();
  }, []);

  // ---------------------------------------------------------------------------
  // Form handlers
  // ---------------------------------------------------------------------------

  function openCreate() {
    setEditingRule(null);
    setFormField('balance');
    setFormOp('gt');
    setFormValue('');
    setFormScore('');
    setFormError('');
    setFormOpen(true);
  }

  function openEdit(rule: ScoringRule) {
    setEditingRule(rule);
    setFormField(rule.field);
    setFormOp(rule.operator);
    setFormValue(rule.value);
    setFormScore(String(rule.score));
    setFormError('');
    setFormOpen(true);
  }

  function closeForm() {
    setFormOpen(false);
    setEditingRule(null);
    setFormError('');
  }

  async function handleSave(e: React.FormEvent) {
    e.preventDefault();
    if (!formValue.trim()) {
      setFormError('Value is required.');
      return;
    }
    if (!formScore.trim() || isNaN(Number(formScore))) {
      setFormError('Score must be a valid number.');
      return;
    }
    setFormSaving(true);
    setFormError('');
    try {
      const payload = {
        field: formField,
        operator: formOp,
        value: formValue.trim(),
        score: parseInt(formScore, 10),
      };
      if (editingRule) {
        await api.put(`/retention/scoring-rules/${editingRule.id}`, payload);
      } else {
        await api.post('/retention/scoring-rules', payload);
      }
      closeForm();
      await loadRules();
    } catch (err: any) {
      setFormError(err.response?.data?.detail || 'Failed to save scoring rule.');
    } finally {
      setFormSaving(false);
    }
  }

  async function handleDelete(rule: ScoringRule) {
    if (!window.confirm(`Delete scoring rule "${FIELD_LABELS[rule.field] || rule.field} ${OP_LABELS[rule.operator] || rule.operator} ${rule.value}"?`)) return;
    try {
      await api.delete(`/retention/scoring-rules/${rule.id}`);
      await loadRules();
    } catch (err: any) {
      alert(err.response?.data?.detail || 'Failed to delete scoring rule.');
    }
  }

  // ---------------------------------------------------------------------------
  // Render
  // ---------------------------------------------------------------------------

  return (
    <div className="space-y-4">
      {/* Top bar */}
      <div className="flex justify-between items-center">
        <h2 className="text-lg font-semibold text-gray-800">Scoring Rules</h2>
        <button
          onClick={openCreate}
          className="px-4 py-2 bg-blue-600 text-white rounded-md text-sm font-medium hover:bg-blue-700 transition-colors"
        >
          New Scoring Rule
        </button>
      </div>

      <p className="text-sm text-gray-500">
        Define rules to score clients in the Retention Manager table. Each rule assigns a score when a condition is met. The total score for each client is the sum of all matching rules.
      </p>

      {/* Create / Edit form */}
      {formOpen && (
        <form
          onSubmit={handleSave}
          className="bg-white rounded-lg shadow p-5 space-y-4"
        >
          <h3 className="text-sm font-semibold text-gray-700">
            {editingRule ? 'Edit Scoring Rule' : 'Create Scoring Rule'}
          </h3>

          <div className="flex items-end gap-3 flex-wrap">
            {/* Field */}
            <div>
              <label className="block text-xs font-medium text-gray-600 mb-1">Field</label>
              <select
                value={formField}
                onChange={(e) => setFormField(e.target.value)}
                className="border border-gray-300 rounded-md px-2 py-1.5 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
              >
                {FIELD_OPTIONS.map((f) => (
                  <option key={f.key} value={f.key}>{f.label}</option>
                ))}
              </select>
            </div>

            {/* Operator */}
            <div>
              <label className="block text-xs font-medium text-gray-600 mb-1">Operator</label>
              <select
                value={formOp}
                onChange={(e) => setFormOp(e.target.value)}
                className="border border-gray-300 rounded-md px-2 py-1.5 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
              >
                {OP_OPTIONS.map((o) => (
                  <option key={o.key} value={o.key}>{o.label}</option>
                ))}
              </select>
            </div>

            {/* Value */}
            <div>
              <label className="block text-xs font-medium text-gray-600 mb-1">Value</label>
              <input
                type="number"
                step="any"
                value={formValue}
                onChange={(e) => setFormValue(e.target.value)}
                placeholder="e.g. 300"
                className="border border-gray-300 rounded-md px-2 py-1.5 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 w-28"
              />
            </div>

            {/* Score */}
            <div>
              <label className="block text-xs font-medium text-gray-600 mb-1">Score</label>
              <input
                type="number"
                value={formScore}
                onChange={(e) => setFormScore(e.target.value)}
                placeholder="e.g. 5"
                className="border border-gray-300 rounded-md px-2 py-1.5 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 w-20"
              />
            </div>
          </div>

          {formError && <p className="text-sm text-red-600">{formError}</p>}

          <div className="flex gap-2">
            <button
              type="submit"
              disabled={formSaving}
              className="px-4 py-1.5 bg-blue-600 text-white rounded-md text-sm font-medium hover:bg-blue-700 disabled:opacity-50 transition-colors"
            >
              {formSaving ? 'Saving...' : 'Save'}
            </button>
            <button
              type="button"
              onClick={closeForm}
              className="px-4 py-1.5 bg-gray-100 text-gray-700 rounded-md text-sm font-medium hover:bg-gray-200 transition-colors"
            >
              Cancel
            </button>
          </div>
        </form>
      )}

      {/* Rules table */}
      <div className="bg-white rounded-lg shadow overflow-hidden">
        {loading ? (
          <div className="p-12 text-center text-gray-400 text-sm">Loading...</div>
        ) : rules.length === 0 ? (
          <div className="p-12 text-center text-gray-400 text-sm">
            No scoring rules yet. Click 'New Scoring Rule' to create one.
          </div>
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full">
              <thead className="bg-gray-50">
                <tr>
                  {['Field', 'Operator', 'Value', 'Score', 'Created', 'Actions'].map((h) => (
                    <th
                      key={h}
                      className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider whitespace-nowrap"
                    >
                      {h}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {rules.map((rule) => (
                  <tr key={rule.id} className="border-t border-gray-100 hover:bg-gray-50">
                    <td className="px-4 py-3 text-sm text-gray-900 font-medium">
                      {FIELD_LABELS[rule.field] || rule.field}
                    </td>
                    <td className="px-4 py-3 text-sm text-gray-700">
                      {OP_LABELS[rule.operator] || rule.operator}
                    </td>
                    <td className="px-4 py-3 text-sm text-gray-700">{rule.value}</td>
                    <td className="px-4 py-3 text-sm font-semibold text-blue-700">{rule.score}</td>
                    <td className="px-4 py-3 text-sm text-gray-500 whitespace-nowrap">
                      {rule.created_at ? new Date(rule.created_at).toLocaleDateString() : '--'}
                    </td>
                    <td className="px-4 py-3 text-sm flex gap-2">
                      <button
                        onClick={() => openEdit(rule)}
                        className="px-3 py-1 text-xs bg-gray-100 text-gray-700 rounded-md hover:bg-gray-200 transition-colors"
                      >
                        Edit
                      </button>
                      <button
                        onClick={() => handleDelete(rule)}
                        className="px-3 py-1 text-xs bg-red-100 text-red-600 rounded-md hover:bg-red-200 transition-colors"
                      >
                        Delete
                      </button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </div>
  );
}
