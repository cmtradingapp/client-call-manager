import { ClientTable } from './components/ClientTable';
import { FilterPanel } from './components/FilterPanel';

export default function App() {
  return (
    <div className="min-h-screen bg-gray-100">
      <header className="bg-white shadow-sm">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-4">
          <h1 className="text-xl font-bold text-gray-900">Client Call Manager</h1>
        </div>
      </header>
      <main className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-6 space-y-6">
        <FilterPanel />
        <ClientTable />
      </main>
    </div>
  );
}
