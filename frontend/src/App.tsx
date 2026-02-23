import { useState } from 'react';

import { CallHistoryTable } from './components/CallHistoryTable';
import { ClientTable } from './components/ClientTable';
import { FilterPanel } from './components/FilterPanel';

type Page = 'call-manager' | 'call-history';

const NAV_SECTIONS = [
  {
    title: 'AI Calls',
    items: [
      { id: 'call-manager' as Page, label: 'Call Manager' },
      { id: 'call-history' as Page, label: 'Call History' },
    ],
  },
];

const PAGE_TITLES: Record<Page, string> = {
  'call-manager': 'Call Manager',
  'call-history': 'Call History',
};

export default function App() {
  const [activePage, setActivePage] = useState<Page>('call-manager');

  return (
    <div className="flex h-screen bg-gray-100 overflow-hidden">
      {/* Sidebar */}
      <aside className="w-56 bg-gray-900 text-white flex flex-col flex-shrink-0">
        {/* Logo / System name */}
        <div className="px-5 py-5 border-b border-gray-700">
          <span className="text-lg font-bold tracking-tight">Back Office</span>
        </div>

        {/* Navigation */}
        <nav className="flex-1 px-3 py-4 space-y-6 overflow-y-auto">
          {NAV_SECTIONS.map((section) => (
            <div key={section.title}>
              <p className="px-2 mb-1 text-xs font-semibold text-gray-400 uppercase tracking-wider">
                {section.title}
              </p>
              <ul className="space-y-0.5">
                {section.items.map((item) => (
                  <li key={item.id}>
                    <button
                      onClick={() => setActivePage(item.id)}
                      className={`w-full text-left px-3 py-2 rounded-md text-sm font-medium transition-colors ${
                        activePage === item.id
                          ? 'bg-blue-600 text-white'
                          : 'text-gray-300 hover:bg-gray-700 hover:text-white'
                      }`}
                    >
                      {item.label}
                    </button>
                  </li>
                ))}
              </ul>
            </div>
          ))}
        </nav>
      </aside>

      {/* Main content */}
      <div className="flex-1 flex flex-col overflow-hidden">
        {/* Top bar */}
        <header className="bg-white shadow-sm px-6 py-4 flex-shrink-0">
          <h1 className="text-lg font-semibold text-gray-800">{PAGE_TITLES[activePage]}</h1>
        </header>

        {/* Page content */}
        <main className="flex-1 overflow-y-auto px-6 py-6 space-y-6">
          {activePage === 'call-manager' && (
            <>
              <FilterPanel />
              <ClientTable />
            </>
          )}
          {activePage === 'call-history' && <CallHistoryTable />}
        </main>
      </div>
    </div>
  );
}
