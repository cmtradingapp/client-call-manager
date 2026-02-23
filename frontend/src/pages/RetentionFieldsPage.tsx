import { useEffect, useState } from 'react';
import axios from 'axios';

const api = axios.create({ baseURL: import.meta.env.VITE_API_BASE_URL || '/api' });
api.interceptors.request.use((config) => {
  const auth = JSON.parse(localStorage.getItem('auth') || '{}');
  const token = auth?.state?.token;
  if (token) config.headers.Authorization = `Bearer ${token}`;
  return config;
});

const TABLES = [
  'report.ant_acc',
  'report.vtiger_trading_accounts',
  'report.dealio_mt4trades',
];

const OPERATORS = [
  { value: '+', label: '+ Add' },
  { value: '-', label: '− Subtract' },
  { value: '*', label: '× Multiply' },
  { value: '/', label: '÷ Divide' },
];

interface RetentionField {
  id: number;
  field_name: string;
  table_a: string;
  column_a: string;
  operator: string;
  table_b: string;
  column_b: string;
  created_at: string;
}

const emptyForm = {
  field_name: '',
  table_a: TABLES[0],
  column_a: '',
  operator: '/',
  table_b: TABLES[0],
  column_b: '',
};

export function RetentionFieldsPage() {
  const [fields, setFields] = useState<RetentionField[]>([]);
  const [loading, setLoading] = useState(true);
  const [showForm, setShowForm] = useState(false);
  const [form, setForm] = useState(emptyForm);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState('');

  const load = async () => {
    try {
      const res = await api.get('/retention-fields');
      setFields(res.data);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => { load(); }, []);

  const resetForm = () => {
    setShowForm(false);
    setForm(emptyForm);
    setError('');
  };

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!form.column_a.trim() || !form.column_b.trim()) {
      setError('Both column names are required');
      return;
    }
    setSaving(true);
    setError('');
    try {
      await api.post('/retention-fields', form);
      resetForm();
      load();
    } catch (err: any) {
      setError(err.response?.data?.detail || 'Failed to save field');
    } finally {
      setSaving(false);
    }
  };

  const deleteField = async (id: number) => {
    if (!confirm('Delete this field? It will be removed from the Retention Manager.')) return;
    try {
      await api.delete(`/retention-fields/${id}`);
      load();
    } catch (err: any) {
      alert(err.response?.data?.detail || 'Failed to delete field');
    }
  };

  const formulaLabel = (f: RetentionField) =>
    `${f.table_a}.${f.column_a} ${f.operator} ${f.table_b}.${f.column_b}`;

  return (
    <div className="space-y-4">
      <div className="flex justify-between items-center">
        <p className="text-sm text-gray-500">{fields.length} field{fields.length !== 1 ? 's' : ''} defined</p>
        <button
          onClick={() => showForm ? resetForm() : setShowForm(true)}
          className="px-4 py-2 bg-blue-600 text-white rounded-md text-sm font-medium hover:bg-blue-700 transition-colors"
        >
          {showForm ? 'Cancel' : '+ Add Field'}
        </button>
      </div>

      {showForm && (
        <form onSubmit={handleSubmit} className="bg-white rounded-lg shadow p-5 space-y-4">
          <h3 className="text-sm font-semibold text-gray-700">New Retention Field</h3>

          {/* Field name */}
          <div>
            <label className="block text-xs font-medium text-gray-600 mb-1">Field Name *</label>
            <input
              required
              value={form.field_name}
              onChange={(e) => setForm({ ...form, field_name: e.target.value })}
              placeholder="e.g. Profit Ratio"
              className="w-full max-w-xs border border-gray-300 rounded-md px-3 py-1.5 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
            />
          </div>

          {/* Formula builder */}
          <div>
            <label className="block text-xs font-medium text-gray-600 mb-2">Formula</label>
            <div className="flex flex-wrap items-end gap-2">
              {/* Column A */}
              <div className="space-y-1">
                <p className="text-xs text-gray-400">Table A</p>
                <select
                  value={form.table_a}
                  onChange={(e) => setForm({ ...form, table_a: e.target.value })}
                  className="border border-gray-300 rounded-md px-2 py-1.5 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
                >
                  {TABLES.map((t) => <option key={t} value={t}>{t}</option>)}
                </select>
              </div>
              <div className="space-y-1">
                <p className="text-xs text-gray-400">Column A *</p>
                <input
                  required
                  value={form.column_a}
                  onChange={(e) => setForm({ ...form, column_a: e.target.value })}
                  placeholder="column_name"
                  className="w-36 border border-gray-300 rounded-md px-2 py-1.5 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
                />
              </div>

              {/* Operator */}
              <div className="space-y-1">
                <p className="text-xs text-gray-400">Operator</p>
                <select
                  value={form.operator}
                  onChange={(e) => setForm({ ...form, operator: e.target.value })}
                  className="border border-gray-300 rounded-md px-2 py-1.5 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
                >
                  {OPERATORS.map((o) => <option key={o.value} value={o.value}>{o.label}</option>)}
                </select>
              </div>

              {/* Column B */}
              <div className="space-y-1">
                <p className="text-xs text-gray-400">Table B</p>
                <select
                  value={form.table_b}
                  onChange={(e) => setForm({ ...form, table_b: e.target.value })}
                  className="border border-gray-300 rounded-md px-2 py-1.5 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
                >
                  {TABLES.map((t) => <option key={t} value={t}>{t}</option>)}
                </select>
              </div>
              <div className="space-y-1">
                <p className="text-xs text-gray-400">Column B *</p>
                <input
                  required
                  value={form.column_b}
                  onChange={(e) => setForm({ ...form, column_b: e.target.value })}
                  placeholder="column_name"
                  className="w-36 border border-gray-300 rounded-md px-2 py-1.5 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
                />
              </div>
            </div>

            {/* Preview */}
            {form.column_a && form.column_b && (
              <p className="mt-2 text-xs text-gray-500">
                Preview: <code className="bg-gray-100 px-1 rounded">{form.table_a}.{form.column_a} {form.operator} {form.table_b}.{form.column_b}</code>
              </p>
            )}
          </div>

          {error && <p className="text-sm text-red-600">{error}</p>}

          <div className="flex gap-2">
            <button
              type="submit"
              disabled={saving}
              className="px-4 py-1.5 bg-blue-600 text-white rounded-md text-sm font-medium hover:bg-blue-700 disabled:opacity-50 transition-colors"
            >
              {saving ? 'Saving…' : 'Add Field'}
            </button>
            <button type="button" onClick={resetForm} className="px-4 py-1.5 bg-gray-100 text-gray-700 rounded-md text-sm font-medium hover:bg-gray-200 transition-colors">
              Cancel
            </button>
          </div>
        </form>
      )}

      <div className="bg-white rounded-lg shadow overflow-hidden">
        {loading ? (
          <div className="p-12 text-center text-gray-400 text-sm">Loading…</div>
        ) : fields.length === 0 ? (
          <div className="p-12 text-center text-gray-400 text-sm">No fields defined yet. Add one to see it as a column in the Retention Manager.</div>
        ) : (
          <table className="w-full">
            <thead className="bg-gray-50">
              <tr>
                {['Field Name', 'Formula', 'Created', 'Actions'].map((h) => (
                  <th key={h} className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">{h}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {fields.map((f) => (
                <tr key={f.id} className="border-t border-gray-100 hover:bg-gray-50">
                  <td className="px-4 py-3 text-sm font-medium text-gray-900">{f.field_name}</td>
                  <td className="px-4 py-3 text-sm text-gray-600">
                    <code className="bg-gray-100 px-1.5 py-0.5 rounded text-xs">{formulaLabel(f)}</code>
                  </td>
                  <td className="px-4 py-3 text-sm text-gray-500 whitespace-nowrap">
                    {new Date(f.created_at).toLocaleDateString()}
                  </td>
                  <td className="px-4 py-3">
                    <button onClick={() => deleteField(f.id)} className="text-xs text-red-500 hover:underline">Delete</button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>
    </div>
  );
}
