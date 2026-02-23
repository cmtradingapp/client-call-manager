import { NavLink, Navigate, Route, Routes } from 'react-router-dom';

import { CallHistoryPage } from './pages/CallHistoryPage';
import { CallManagerPage } from './pages/CallManagerPage';
import { RetentionPage } from './pages/RetentionPage';

const NAV_SECTIONS = [
  {
    title: 'AI Calls',
    items: [
      { to: '/call-manager', label: 'Call Manager' },
      { to: '/call-history', label: 'Call History' },
    ],
  },
  {
    title: 'Retention',
    items: [
      { to: '/retention', label: 'Retention Manager' },
    ],
  },
];

const PAGE_TITLES: Record<string, string> = {
  '/call-manager': 'Call Manager',
  '/call-history': 'Call History',
  '/retention': 'Retention Manager',
};

export default function App() {
  return (
    <div className="flex h-screen bg-gray-100 overflow-hidden">
      {/* Sidebar */}
      <aside className="w-56 bg-gray-900 text-white flex flex-col flex-shrink-0">
        <div className="px-5 py-5 border-b border-gray-700">
          <span className="text-lg font-bold tracking-tight">Back Office</span>
        </div>
        <nav className="flex-1 px-3 py-4 space-y-6 overflow-y-auto">
          {NAV_SECTIONS.map((section) => (
            <div key={section.title}>
              <p className="px-2 mb-1 text-xs font-semibold text-gray-400 uppercase tracking-wider">
                {section.title}
              </p>
              <ul className="space-y-0.5">
                {section.items.map((item) => (
                  <li key={item.to}>
                    <NavLink
                      to={item.to}
                      className={({ isActive }) =>
                        `block px-3 py-2 rounded-md text-sm font-medium transition-colors ${
                          isActive
                            ? 'bg-blue-600 text-white'
                            : 'text-gray-300 hover:bg-gray-700 hover:text-white'
                        }`
                      }
                    >
                      {item.label}
                    </NavLink>
                  </li>
                ))}
              </ul>
            </div>
          ))}
        </nav>
      </aside>

      {/* Main content */}
      <div className="flex-1 flex flex-col overflow-hidden">
        <Routes>
          {Object.entries(PAGE_TITLES).map(([path, title]) => (
            <Route
              key={path}
              path={path}
              element={
                <>
                  <header className="bg-white shadow-sm px-6 py-4 flex-shrink-0">
                    <h1 className="text-lg font-semibold text-gray-800">{title}</h1>
                  </header>
                  <main className="flex-1 overflow-y-auto px-6 py-6 space-y-6">
                    {path === '/call-manager' && <CallManagerPage />}
                    {path === '/call-history' && <CallHistoryPage />}
                    {path === '/retention' && <RetentionPage />}
                  </main>
                </>
              }
            />
          ))}
          <Route path="*" element={<Navigate to="/call-manager" replace />} />
        </Routes>
      </div>
    </div>
  );
}
