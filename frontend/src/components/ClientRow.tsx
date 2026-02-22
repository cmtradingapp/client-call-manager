import type { CallStatusType, ClientDetail } from '../types';

interface StatusBadgeProps {
  status: string;
}

function StatusBadge({ status }: StatusBadgeProps) {
  const colors: Record<string, string> = {
    active: 'bg-green-100 text-green-800',
    inactive: 'bg-gray-100 text-gray-700',
    pending: 'bg-yellow-100 text-yellow-800',
  };
  return (
    <span
      className={`px-2 py-0.5 rounded-full text-xs font-medium ${
        colors[status] ?? 'bg-gray-100 text-gray-700'
      }`}
    >
      {status}
    </span>
  );
}

interface CallStatusBadgeProps {
  status: CallStatusType;
}

function CallStatusBadge({ status }: CallStatusBadgeProps) {
  if (status === 'idle') return null;
  const styles: Record<string, string> = {
    calling: 'bg-blue-100 text-blue-800',
    initiated: 'bg-green-100 text-green-800',
    failed: 'bg-red-100 text-red-800',
  };
  const labels: Record<string, string> = {
    calling: 'Calling…',
    initiated: 'Called',
    failed: 'Failed',
  };
  return (
    <span className={`px-2 py-0.5 rounded-full text-xs font-medium ${styles[status]}`}>
      {labels[status]}
    </span>
  );
}

interface ClientRowProps {
  client: ClientDetail;
  selected: boolean;
  callStatus: CallStatusType;
  onToggle: () => void;
}

export function ClientRow({ client, selected, callStatus, onToggle }: ClientRowProps) {
  return (
    <tr
      className={`border-b border-gray-100 hover:bg-gray-50 transition-colors ${
        selected ? 'bg-blue-50' : ''
      }`}
    >
      <td className="px-4 py-3">
        <input
          type="checkbox"
          checked={selected}
          onChange={onToggle}
          className="rounded border-gray-300 text-blue-600 focus:ring-blue-500"
        />
      </td>
      <td className="px-4 py-3 text-sm font-medium text-gray-900">{client.client_id}</td>
      <td className="px-4 py-3 text-sm text-gray-700">{client.name}</td>
      <td className="px-4 py-3 text-sm">
        <StatusBadge status={client.status} />
      </td>
      <td className="px-4 py-3 text-sm text-gray-600">{client.region ?? '—'}</td>
      <td className="px-4 py-3 text-sm text-gray-600">{client.language ?? '—'}</td>
      <td className="px-4 py-3 text-sm text-gray-600">
        {client.sales_client_potential ?? '—'}
      </td>
      <td className="px-4 py-3 text-sm text-gray-600">{client.phone_number ?? '—'}</td>
      <td className="px-4 py-3 text-sm text-gray-600">{client.email ?? '—'}</td>
      <td className="px-4 py-3 text-sm">
        <CallStatusBadge status={callStatus} />
      </td>
    </tr>
  );
}
