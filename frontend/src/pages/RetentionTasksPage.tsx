export function RetentionTasksPage() {
  return (
    <div className="bg-white rounded-lg shadow overflow-hidden">
      <div className="px-4 py-3 border-b border-gray-200 bg-gray-50">
        <span className="text-sm text-gray-600">Retention Tasks</span>
      </div>
      <div className="overflow-x-auto">
        <table className="w-full">
          <thead className="bg-gray-50">
            <tr>
              {['Client ID', 'Task'].map((h) => (
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
            <tr>
              <td colSpan={2} className="px-4 py-12 text-center text-sm text-gray-400">
                No retention tasks yet.
              </td>
            </tr>
          </tbody>
        </table>
      </div>
    </div>
  );
}
