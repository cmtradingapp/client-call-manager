import { useEffect, useState } from 'react';
import axios from 'axios';

const api = axios.create({ baseURL: import.meta.env.VITE_API_BASE_URL || '/api' });
api.interceptors.request.use((config) => {
  const auth = JSON.parse(localStorage.getItem('auth') || '{}');
  const token = auth?.state?.token;
  if (token) config.headers.Authorization = `Bearer ${token}`;
  return config;
});

interface Integration {
  id: number;
  name: string;
  base_url: string;
  auth_key: string | null;
  description: string | null;
  is_active: boolean;
  created_at: string;
}

interface DbInfo {
  host: string;
  port?: number;
  database: string;
  user: string;
  status: string;
}

interface IntegrationsResponse {
  integrations: Integration[];
  databases: Record<string, DbInfo>;
}

const EMPTY_FORM = { name: '', base_url: '', auth_key: '', description: '', is_active: true };

export function IntegrationsPage() {
  const [data, setData] = useState<IntegrationsResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [showForm, setShowForm] = useState(false);
  const [editId, setEditId] = useState<number | null>(null);
  const [form, setForm] = useState({ ...EMPTY_FORM });
  const [saving, setSaving] = useState(false);
  const [formError, setFormError] = useState('');
  const [revealedKeys, setRevealedKeys] = useState<Set<number>>(new Set());
  const [revealedFullKeys, setRevealedFullKeys] = useState<Record<number, string>>({});

  const load = async () => {
    try {
      const res = await api.get<IntegrationsResponse>('/admin/integrations');
      setData(res.data);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => { load(); }, []);

  const toggleRevealKey = async (id: number) => {
    if (revealedKeys.has(id)) {
      setRevealedKeys((prev) => { const n = new Set(prev); n.delete(id); return n; });
      return;
    }
    // Fetch the unmasked key
    try {
      const res = await api.get<Integration>(`/admin/integrations/${id}?reveal_key=true`);
      setRevealedFullKeys((prev) => ({ ...prev, [id]: res.data.auth_key || '' }));
      setRevealedKeys((prev) => new Set(prev).add(id));
    } catch {
      // fallback: just toggle with masked key
      setRevealedKeys((prev) => new Set(prev).add(id));
    }
  };

  const startEdit = (integration: Integration) => {
    setEditId(integration.id);
    setForm({
      name: integration.name,
      base_url: integration.base_url,
      auth_key: '', // don't pre-fill masked key
      description: integration.description || '',
      is_active: integration.is_active,
    });
    setShowForm(true);
  };

  const resetForm = () => {
    setShowForm(false);
    setEditId(null);
    setForm({ ...EMPTY_FORM });
    setFormError('');
  };

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setSaving(true);
    setFormError('');
    try {
      const payload = {
        name: form.name,
        base_url: form.base_url,
        auth_key: form.auth_key || null,
        description: form.description || null,
        is_active: form.is_active,
      };
      if (editId) {
        await api.put(`/admin/integrations/${editId}`, payload);
      } else {
        await api.post('/admin/integrations', payload);
      }
      resetForm();
      load();
    } catch (err: any) {
      setFormError(err.response?.data?.detail || 'Failed to save integration');
    } finally {
      setSaving(false);
    }
  };

  const deleteIntegration = async (id: number) => {
    if (!confirm('Delete this integration?')) return;
    try {
      await api.delete(`/admin/integrations/${id}`);
      load();
    } catch (err: any) {
      alert(err.response?.data?.detail || 'Failed to delete integration');
    }
  };

  const statusBadge = (status: string) => {
    const colors: Record<string, string> = {
      connected: 'bg-green-100 text-green-800',
      configured: 'bg-blue-100 text-blue-800',
      local: 'bg-yellow-100 text-yellow-800',
    };
    return (
      <span className={`px-2 py-0.5 rounded-full text-xs font-medium ${colors[status] || 'bg-gray-100 text-gray-700'}`}>
        {status}
      </span>
    );
  };

  if (loading) {
    return <div className="text-center text-gray-400 py-12">Loading...</div>;
  }

  const integrations = data?.integrations || [];
  const databases = data?.databases || {};

  return (
    <div className="space-y-6">
      {/* Integrations Section */}
      <div>
        <div className="flex justify-between items-center mb-4">
          <h2 className="text-base font-semibold text-gray-700">API Integrations</h2>
          <button
            onClick={() => showForm && !editId ? resetForm() : setShowForm(true)}
            className="px-4 py-2 bg-blue-600 text-white rounded-md text-sm font-medium hover:bg-blue-700 transition-colors"
          >
            {showForm && !editId ? 'Cancel' : '+ Add Integration'}
          </button>
        </div>

        {/* Add/Edit Form */}
        {showForm && (
          <form onSubmit={handleSubmit} className="bg-white rounded-lg shadow p-5 space-y-4 mb-4">
            <h3 className="text-sm font-semibold text-gray-700">
              {editId ? 'Edit Integration' : 'New Integration'}
            </h3>
            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              <div>
                <label className="block text-xs font-medium text-gray-600 mb-1">Name *</label>
                <input
                  required
                  value={form.name}
                  onChange={(e) => setForm({ ...form, name: e.target.value })}
                  placeholder="e.g. SendGrid, Optimove"
                  className="w-full border border-gray-300 rounded-md px-3 py-1.5 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
                />
              </div>
              <div>
                <label className="block text-xs font-medium text-gray-600 mb-1">Base URL *</label>
                <input
                  required
                  value={form.base_url}
                  onChange={(e) => setForm({ ...form, base_url: e.target.value })}
                  placeholder="https://api.example.com/v1/"
                  className="w-full border border-gray-300 rounded-md px-3 py-1.5 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
                />
              </div>
              <div>
                <label className="block text-xs font-medium text-gray-600 mb-1">Auth Key</label>
                <input
                  value={form.auth_key}
                  onChange={(e) => setForm({ ...form, auth_key: e.target.value })}
                  placeholder={editId ? '(leave blank to keep current)' : 'Optional'}
                  className="w-full border border-gray-300 rounded-md px-3 py-1.5 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
                />
              </div>
              <div>
                <label className="block text-xs font-medium text-gray-600 mb-1">Description</label>
                <input
                  value={form.description}
                  onChange={(e) => setForm({ ...form, description: e.target.value })}
                  placeholder="Optional description"
                  className="w-full border border-gray-300 rounded-md px-3 py-1.5 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
                />
              </div>
            </div>
            <div>
              <label className="flex items-center gap-2 text-sm text-gray-700 cursor-pointer">
                <input
                  type="checkbox"
                  checked={form.is_active}
                  onChange={(e) => setForm({ ...form, is_active: e.target.checked })}
                  className="rounded border-gray-300 text-blue-600 focus:ring-blue-500"
                />
                Active
              </label>
            </div>
            {formError && <p className="text-sm text-red-600">{formError}</p>}
            <div className="flex gap-2">
              <button
                type="submit"
                disabled={saving}
                className="px-4 py-1.5 bg-blue-600 text-white rounded-md text-sm font-medium hover:bg-blue-700 disabled:opacity-50 transition-colors"
              >
                {saving ? 'Saving...' : editId ? 'Update' : 'Create'}
              </button>
              <button
                type="button"
                onClick={resetForm}
                className="px-4 py-1.5 bg-gray-100 text-gray-700 rounded-md text-sm font-medium hover:bg-gray-200 transition-colors"
              >
                Cancel
              </button>
            </div>
          </form>
        )}

        {/* Integration Cards */}
        {integrations.length === 0 ? (
          <div className="bg-white rounded-lg shadow p-8 text-center text-gray-400 text-sm">
            No integrations configured yet. Click "+ Add Integration" to get started.
          </div>
        ) : (
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
            {integrations.map((integ) => (
              <div key={integ.id} className="bg-white rounded-lg shadow p-5 space-y-3">
                <div className="flex justify-between items-start">
                  <div>
                    <h3 className="text-sm font-semibold text-gray-800">{integ.name}</h3>
                    {integ.description && (
                      <p className="text-xs text-gray-500 mt-0.5">{integ.description}</p>
                    )}
                  </div>
                  <span
                    className={`px-2 py-0.5 rounded-full text-xs font-medium ${
                      integ.is_active
                        ? 'bg-green-100 text-green-800'
                        : 'bg-red-100 text-red-800'
                    }`}
                  >
                    {integ.is_active ? 'Active' : 'Inactive'}
                  </span>
                </div>

                <div className="space-y-1.5 text-sm">
                  <div className="flex items-center gap-2">
                    <span className="text-xs font-medium text-gray-500 w-20 flex-shrink-0">Base URL</span>
                    <span className="text-gray-700 break-all">{integ.base_url}</span>
                  </div>
                  {integ.auth_key && (
                    <div className="flex items-center gap-2">
                      <span className="text-xs font-medium text-gray-500 w-20 flex-shrink-0">Auth Key</span>
                      <code className="text-xs bg-gray-50 px-2 py-0.5 rounded text-gray-600 break-all">
                        {revealedKeys.has(integ.id)
                          ? revealedFullKeys[integ.id] || integ.auth_key
                          : integ.auth_key}
                      </code>
                      <button
                        onClick={() => toggleRevealKey(integ.id)}
                        className="text-xs text-blue-600 hover:underline flex-shrink-0"
                      >
                        {revealedKeys.has(integ.id) ? 'Hide' : 'Reveal'}
                      </button>
                    </div>
                  )}
                  <div className="flex items-center gap-2">
                    <span className="text-xs font-medium text-gray-500 w-20 flex-shrink-0">Added</span>
                    <span className="text-gray-500 text-xs">
                      {new Date(integ.created_at).toLocaleDateString()}
                    </span>
                  </div>
                </div>

                <div className="flex gap-2 pt-1 border-t border-gray-100">
                  <button
                    onClick={() => startEdit(integ)}
                    className="text-xs text-blue-600 hover:underline"
                  >
                    Edit
                  </button>
                  <button
                    onClick={() => deleteIntegration(integ.id)}
                    className="text-xs text-red-500 hover:underline"
                  >
                    Delete
                  </button>
                </div>
              </div>
            ))}
          </div>
        )}
      </div>

      {/* Database Connections Section */}
      <div>
        <h2 className="text-base font-semibold text-gray-700 mb-4">Database Connections</h2>
        <div className="grid grid-cols-1 lg:grid-cols-2 xl:grid-cols-3 gap-4">
          {Object.entries(databases).map(([key, db]) => (
            <div key={key} className="bg-white rounded-lg shadow p-5 space-y-3">
              <div className="flex justify-between items-center">
                <h3 className="text-sm font-semibold text-gray-800 uppercase">{key}</h3>
                {statusBadge(db.status)}
              </div>
              <div className="space-y-1.5 text-sm">
                <div className="flex items-center gap-2">
                  <span className="text-xs font-medium text-gray-500 w-16 flex-shrink-0">Host</span>
                  <span className="text-gray-700">{db.host}</span>
                </div>
                {db.port && (
                  <div className="flex items-center gap-2">
                    <span className="text-xs font-medium text-gray-500 w-16 flex-shrink-0">Port</span>
                    <span className="text-gray-700">{db.port}</span>
                  </div>
                )}
                <div className="flex items-center gap-2">
                  <span className="text-xs font-medium text-gray-500 w-16 flex-shrink-0">Database</span>
                  <span className="text-gray-700">{db.database}</span>
                </div>
                <div className="flex items-center gap-2">
                  <span className="text-xs font-medium text-gray-500 w-16 flex-shrink-0">User</span>
                  <span className="text-gray-700">{db.user}</span>
                </div>
              </div>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}
