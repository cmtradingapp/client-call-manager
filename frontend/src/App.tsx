import { useState } from 'react';

import { CallHistoryTable } from './components/CallHistoryTable';
import { ClientTable } from './components/ClientTable';
import { FilterPanel } from './components/FilterPanel';

type Tab = 'search' | 'history';

export default function App() {
  const [activeTab, setActiveTab] = useState<Tab>('search');

  return (
    <div className="min-h-screen bg-gray-100">
      <header className="bg-white shadow-sm">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-4">
          <h1 className="text-xl font-bold text-gray-900">Client Call Manager</h1>
        </div>
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
          <nav className="flex border-b border-gray-200">
            {([
              { id: 'search', label: 'Search & Call' },
              { id: 'history', label: 'Call History' },
            ] as { id: Tab; label: string }[]).map((tab) => (
              <button
                key={tab.id}
                onClick={() => setActiveTab(tab.id)}
                className={`px-6 py-3 text-sm font-medium border-b-2 transition-colors ${
                  activeTab === tab.id
                    ? 'border-blue-600 text-blue-600'
                    : 'border-transparent text-gray-500 hover:text-gray-700'
                }`}
              >
                {tab.label}
              </button>
            ))}
          </nav>
        </div>
      </header>
      <main className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-6 space-y-6">
        {activeTab === 'search' ? (
          <>
            <FilterPanel />
            <ClientTable />
          </>
        ) : (
          <CallHistoryTable />
        )}
      </main>
    </div>
  );
}
