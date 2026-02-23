import { useRef, useState } from 'react';
import axios from 'axios';

import { initiateCalls } from '../api/client';

const api = axios.create({ baseURL: import.meta.env.VITE_API_BASE_URL || '/api' });
api.interceptors.request.use((config) => {
  const auth = JSON.parse(localStorage.getItem('auth') || '{}');
  const token = auth?.state?.token;
  if (token) config.headers.Authorization = `Bearer ${token}`;
  return config;
});

type CallStatus = 'idle' | 'calling' | 'called' | 'failed';

interface BatchClient {
  id: string;
  first_name: string;
  email: string;
  phone?: string;
  error?: string;
  callStatus: CallStatus;
}

interface Summary {
  total: number;
  ready: number;
  errors: number;
}

function parseCSV(text: string): { id: string }[] {
  const lines = text.trim().split(/\r?\n/);
  if (lines.length < 1) return [];
  // Support files with or without a header row
  const firstLine = lines[0].trim().toLowerCase().replace(/"/g, '');
  const dataLines = firstLine === 'id' ? lines.slice(1) : lines;
  return dataLines
    .map((line) => ({ id: line.split(',')[0].trim().replace(/"/g, '') }))
    .filter((r) => r.id);
}

const maskPhone = (phone?: string) => {
  if (!phone) return '—';
  return phone.slice(0, 5) + '*'.repeat(Math.max(0, phone.length - 5));
};

export function BatchCallPage() {
  const [agentId, setAgentId] = useState('');
  const [agentPhoneNumberId, setAgentPhoneNumberId] = useState('');
  const [clients, setClients] = useState<BatchClient[]>([]);
  const [summary, setSummary] = useState<Summary | null>(null);
  const [loading, setLoading] = useState(false);
  const [calling, setCalling] = useState(false);
  const [uploadError, setUploadError] = useState('');
  const fileRef = useRef<HTMLInputElement>(null);

  const handleFile = async (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (!file) return;
    setUploadError('');
    setClients([]);
    setSummary(null);

    const text = await file.text();
    const parsed = parseCSV(text);

    if (parsed.length === 0) {
      setUploadError('No valid IDs found. Make sure the CSV has an id column.');
      return;
    }

    setLoading(true);
    try {
      const res = await api.post('/clients/lookup', { clients: parsed });
      const enriched: BatchClient[] = res.data.map((r: any) => ({
        id: r.id,
        first_name: r.first_name ?? '',
        email: r.email ?? '',
        phone: r.phone ?? undefined,
        error: r.error ?? undefined,
        callStatus: 'idle' as CallStatus,
      }));
      setClients(enriched);
      setSummary({
        total: enriched.length,
        ready: enriched.filter((c) => c.phone).length,
        errors: enriched.filter((c) => !c.phone).length,
      });
    } catch {
      setUploadError('Failed to look up clients from CRM');
    } finally {
      setLoading(false);
      if (fileRef.current) fileRef.current.value = '';
    }
  };

  const callAll = async () => {
    const readyIds = clients.filter((c) => c.phone && c.callStatus === 'idle').map((c) => c.id);
    if (readyIds.length === 0) return;

    setCalling(true);
    setClients((prev) =>
      prev.map((c) => (readyIds.includes(c.id) ? { ...c, callStatus: 'calling' } : c))
    );

    try {
      const response = await initiateCalls(readyIds, agentId, agentPhoneNumberId);
      setClients((prev) =>
        prev.map((c) => {
          const result = response.results.find((r) => r.client_id === c.id);
          if (!result) return c;
          return {
            ...c,
            callStatus: result.status === 'initiated' ? 'called' : 'failed',
          };
        })
      );
    } catch {
      setClients((prev) =>
        prev.map((c) => (readyIds.includes(c.id) ? { ...c, callStatus: 'failed' } : c))
      );
    } finally {
      setCalling(false);
    }
  };

  const readyCount = clients.filter((c) => c.phone && c.callStatus === 'idle').length;

  return (
    <div className="space-y-5">
      {/* Settings */}
      <div className="bg-white rounded-lg shadow p-5 space-y-4">
        <h2 className="text-sm font-semibold text-gray-700">Agent Settings</h2>
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          <div>
            <label className="block text-xs font-medium text-gray-600 mb-1">Agent ID</label>
            <input
              type="text"
              placeholder="agent_..."
              value={agentId}
              onChange={(e) => setAgentId(e.target.value)}
              className="w-full border border-gray-300 rounded-md px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
            />
          </div>
          <div>
            <label className="block text-xs font-medium text-gray-600 mb-1">Agent Phone Number ID</label>
            <input
              type="text"
              placeholder="phnum_..."
              value={agentPhoneNumberId}
              onChange={(e) => setAgentPhoneNumberId(e.target.value)}
              className="w-full border border-gray-300 rounded-md px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
            />
          </div>
        </div>

        <div>
          <label className="block text-xs font-medium text-gray-600 mb-1">
            Upload CSV <span className="text-gray-400 font-normal">(one ID per row — first name, email and phone fetched from CRM)</span>
          </label>
          <input
            ref={fileRef}
            type="file"
            accept=".csv"
            onChange={handleFile}
            disabled={loading}
            className="block w-full text-sm text-gray-600 file:mr-3 file:py-1.5 file:px-4 file:rounded-md file:border-0 file:text-sm file:font-medium file:bg-blue-50 file:text-blue-700 hover:file:bg-blue-100 disabled:opacity-50"
          />
          {uploadError && <p className="mt-1 text-xs text-red-600">{uploadError}</p>}
        </div>
      </div>

      {/* Summary */}
      {summary && (
        <div className="grid grid-cols-3 gap-4">
          <div className="bg-white rounded-lg shadow p-4 text-center">
            <p className="text-2xl font-bold text-gray-800">{summary.total}</p>
            <p className="text-xs text-gray-500 mt-1">Total Imported</p>
          </div>
          <div className="bg-white rounded-lg shadow p-4 text-center">
            <p className="text-2xl font-bold text-green-600">{summary.ready}</p>
            <p className="text-xs text-gray-500 mt-1">Ready to Call</p>
          </div>
          <div className="bg-white rounded-lg shadow p-4 text-center">
            <p className="text-2xl font-bold text-red-500">{summary.errors}</p>
            <p className="text-xs text-gray-500 mt-1">Errors (No Phone)</p>
          </div>
        </div>
      )}

      {/* Table */}
      {clients.length > 0 && (
        <div className="bg-white rounded-lg shadow overflow-hidden">
          <div className="px-4 py-3 border-b border-gray-200 bg-gray-50 flex items-center justify-between">
            <span className="text-sm text-gray-600">{clients.length} clients loaded</span>
            {readyCount > 0 && (
              <button
                onClick={callAll}
                disabled={calling}
                className="px-4 py-2 bg-green-600 text-white rounded-md text-sm font-medium hover:bg-green-700 disabled:opacity-50 transition-colors"
              >
                {calling ? 'Calling…' : `Call All (${readyCount})`}
              </button>
            )}
          </div>

          {loading ? (
            <div className="p-12 text-center text-gray-400 text-sm">Looking up clients in CRM…</div>
          ) : (
            <div className="overflow-x-auto">
              <table className="w-full">
                <thead className="bg-gray-50">
                  <tr>
                    {['ID', 'First Name', 'Email', 'Phone', 'Status'].map((h) => (
                      <th key={h} className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                        {h}
                      </th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {clients.map((c) => (
                    <tr key={c.id} className={`border-b border-gray-100 ${!c.phone ? 'bg-red-50 opacity-60' : 'hover:bg-gray-50'}`}>
                      <td className="px-4 py-3 text-sm font-medium text-gray-900">{c.id}</td>
                      <td className="px-4 py-3 text-sm text-gray-700">{c.first_name || '—'}</td>
                      <td className="px-4 py-3 text-sm text-gray-600">{c.email || '—'}</td>
                      <td className="px-4 py-3 text-sm text-gray-600">
                        {c.error ? (
                          <span className="text-red-500 text-xs">{c.error}</span>
                        ) : (
                          maskPhone(c.phone)
                        )}
                      </td>
                      <td className="px-4 py-3 text-sm">
                        {c.callStatus === 'idle' && c.phone && (
                          <span className="px-2 py-0.5 rounded-full text-xs font-medium bg-gray-100 text-gray-600">Ready</span>
                        )}
                        {c.callStatus === 'calling' && (
                          <span className="px-2 py-0.5 rounded-full text-xs font-medium bg-blue-100 text-blue-800">Calling…</span>
                        )}
                        {c.callStatus === 'called' && (
                          <span className="px-2 py-0.5 rounded-full text-xs font-medium bg-green-100 text-green-800">Called ✓</span>
                        )}
                        {c.callStatus === 'failed' && (
                          <span className="px-2 py-0.5 rounded-full text-xs font-medium bg-red-100 text-red-800">Failed</span>
                        )}
                        {c.callStatus === 'idle' && !c.phone && (
                          <span className="px-2 py-0.5 rounded-full text-xs font-medium bg-red-100 text-red-700">No Phone</span>
                        )}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </div>
      )}
    </div>
  );
}
