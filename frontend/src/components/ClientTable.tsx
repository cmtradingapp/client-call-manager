import { useAppStore } from '../store/useAppStore';
import type { CallStatusType } from '../types';
import { CallButton } from './CallButton';
import { ClientRow } from './ClientRow';

export function ClientTable() {
  const {
    results,
    selectedIds,
    conversationIds,
    toggleSelected,
    selectAll,
    deselectAll,
    callStatuses,
    searchError,
    isSearching,
    agentId,
    setAgentId,
    agentPhoneNumberId,
    setAgentPhoneNumberId,
  } = useAppStore();

  const uncalledResults = results.filter((r) => !conversationIds[r.client_id]);
  const allUncalledSelected =
    uncalledResults.length > 0 && uncalledResults.every((r) => selectedIds.has(r.client_id));

  if (isSearching) {
    return (
      <div className="bg-white rounded-lg shadow p-12 text-center">
        <p className="text-gray-500 text-sm">Searching…</p>
      </div>
    );
  }

  if (searchError) {
    return (
      <div className="bg-white rounded-lg shadow p-6">
        <p className="text-red-600 text-sm">{searchError}</p>
      </div>
    );
  }

  if (results.length === 0) {
    return (
      <div className="bg-white rounded-lg shadow p-12 text-center">
        <p className="text-gray-400 text-sm">
          No results. Apply filters above and click Search.
        </p>
      </div>
    );
  }

  return (
    <div className="bg-white rounded-lg shadow overflow-hidden">
      {/* Agent settings */}
      <div className="px-4 py-3 border-b border-gray-200 bg-gray-50 flex flex-wrap gap-3 items-center">
        <span className="text-xs font-medium text-gray-500 uppercase tracking-wide">Agent Settings</span>
        <input
          type="text"
          placeholder="Agent ID"
          value={agentId}
          onChange={(e) => setAgentId(e.target.value)}
          className="flex-1 min-w-48 border border-gray-300 rounded-md px-3 py-1.5 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
        />
        <input
          type="text"
          placeholder="Agent Phone Number ID"
          value={agentPhoneNumberId}
          onChange={(e) => setAgentPhoneNumberId(e.target.value)}
          className="flex-1 min-w-48 border border-gray-300 rounded-md px-3 py-1.5 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
        />
      </div>

      {/* Toolbar */}
      <div className="flex items-center justify-between px-4 py-3 border-b border-gray-200">
        <span className="text-sm text-gray-600">
          {results.length} client{results.length !== 1 ? 's' : ''} found
          {selectedIds.size > 0 && ` · ${selectedIds.size} selected`}
        </span>
        <CallButton />
      </div>

      {/* Table */}
      <div className="overflow-x-auto">
        <table className="w-full">
          <thead className="bg-gray-50">
            <tr>
              <th className="px-4 py-3 text-left">
                <input
                  type="checkbox"
                  checked={allUncalledSelected}
                  onChange={allUncalledSelected ? deselectAll : selectAll}
                  className="rounded border-gray-300 text-blue-600 focus:ring-blue-500"
                />
              </th>
              {['ID', 'Name', 'Status', 'Country', 'Language', 'Potential', 'Phone', 'Email', 'Call Status'].map(
                (h) => (
                  <th
                    key={h}
                    className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider"
                  >
                    {h}
                  </th>
                )
              )}
            </tr>
          </thead>
          <tbody>
            {results.map((client) => (
              <ClientRow
                key={client.client_id}
                client={client}
                selected={selectedIds.has(client.client_id)}
                callStatus={(callStatuses[client.client_id] ?? 'idle') as CallStatusType}
                conversationId={conversationIds[client.client_id]}
                onToggle={() => toggleSelected(client.client_id)}
              />
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
