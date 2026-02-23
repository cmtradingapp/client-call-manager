import { useEffect, useState } from 'react';

import { getCallHistory } from '../api/client';
import type { ElevenLabsConversation } from '../types';

const STATUS_OPTIONS = [
  { value: '', label: 'All' },
  { value: 'success', label: 'Success' },
  { value: 'failure', label: 'Failure' },
  { value: 'unknown', label: 'Unknown' },
];

function CallSuccessBadge({ value }: { value?: string }) {
  if (!value) return <span className="text-gray-400 text-xs">—</span>;
  const styles: Record<string, string> = {
    success: 'bg-green-100 text-green-800',
    failure: 'bg-red-100 text-red-800',
    unknown: 'bg-gray-100 text-gray-600',
  };
  const labels: Record<string, string> = {
    success: 'Success',
    failure: 'Failure',
    unknown: 'Unknown',
  };
  return (
    <span className={`px-2 py-0.5 rounded-full text-xs font-medium ${styles[value] ?? styles.unknown}`}>
      {labels[value] ?? value}
    </span>
  );
}

export function CallHistoryTable() {
  const [conversations, setConversations] = useState<ElevenLabsConversation[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [agentId, setAgentId] = useState('');
  const [callSuccessful, setCallSuccessful] = useState('');

  const load = async () => {
    setLoading(true);
    setError(null);
    try {
      const data = await getCallHistory({
        agent_id: agentId || undefined,
        call_successful: callSuccessful || undefined,
      });
      setConversations(data.conversations ?? []);
    } catch {
      setError('Failed to load call history from ElevenLabs');
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    load();
  }, []);

  const formatDuration = (secs?: number) => {
    if (secs == null) return '—';
    const m = Math.floor(secs / 60);
    const s = secs % 60;
    return m > 0 ? `${m}m ${s}s` : `${s}s`;
  };

  const formatDate = (unixSec?: number) => {
    if (!unixSec) return '—';
    return new Date(unixSec * 1000).toLocaleString();
  };

  return (
    <div className="space-y-4">
      {/* Filters */}
      <div className="bg-white rounded-lg shadow p-4 flex flex-wrap gap-3 items-end">
        <div className="flex-1 min-w-48">
          <label className="block text-xs font-medium text-gray-600 mb-1">Agent ID</label>
          <input
            type="text"
            placeholder="e.g. agent_0101khtww71ve…"
            value={agentId}
            onChange={(e) => setAgentId(e.target.value)}
            className="w-full border border-gray-300 rounded-md px-3 py-1.5 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
          />
        </div>
        <div>
          <label className="block text-xs font-medium text-gray-600 mb-1">Call Result</label>
          <select
            value={callSuccessful}
            onChange={(e) => setCallSuccessful(e.target.value)}
            className="border border-gray-300 rounded-md px-3 py-1.5 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
          >
            {STATUS_OPTIONS.map((o) => (
              <option key={o.value} value={o.value}>{o.label}</option>
            ))}
          </select>
        </div>
        <button
          onClick={load}
          disabled={loading}
          className="px-4 py-1.5 bg-blue-600 text-white rounded-md text-sm font-medium hover:bg-blue-700 disabled:opacity-50 transition-colors"
        >
          {loading ? 'Loading…' : 'Search'}
        </button>
      </div>

      {/* Table */}
      <div className="bg-white rounded-lg shadow overflow-hidden">
        {error ? (
          <div className="p-6 text-red-600 text-sm">{error}</div>
        ) : loading ? (
          <div className="p-12 text-center text-gray-400 text-sm">Loading…</div>
        ) : conversations.length === 0 ? (
          <div className="p-12 text-center text-gray-400 text-sm">No conversations found.</div>
        ) : (
          <>
            <div className="px-4 py-3 border-b border-gray-200 bg-gray-50">
              <span className="text-sm text-gray-600">
                {conversations.length} conversation{conversations.length !== 1 ? 's' : ''}
              </span>
            </div>
            <div className="overflow-x-auto">
              <table className="w-full">
                <thead className="bg-gray-50">
                  <tr>
                    {['Date', 'Conversation ID', 'Agent Name', 'Duration', 'Result'].map((h) => (
                      <th
                        key={h}
                        className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider"
                      >
                        {h}
                      </th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {conversations.map((c) => (
                    <tr key={c.conversation_id} className="border-b border-gray-100 hover:bg-gray-50">
                      <td className="px-4 py-3 text-sm text-gray-700 whitespace-nowrap">
                        {formatDate(c.start_time_unix_secs)}
                      </td>
                      <td className="px-4 py-3 text-sm text-gray-500 font-mono text-xs">
                        {c.conversation_id}
                      </td>
                      <td className="px-4 py-3 text-sm text-gray-700">{c.agent_name ?? '—'}</td>
                      <td className="px-4 py-3 text-sm text-gray-600">
                        {formatDuration(c.call_duration_secs)}
                      </td>
                      <td className="px-4 py-3 text-sm">
                        <CallSuccessBadge value={c.call_successful} />
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </>
        )}
      </div>
    </div>
  );
}
