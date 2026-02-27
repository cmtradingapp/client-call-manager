import type { ReactNode } from 'react';
import { useState } from 'react';
import { NavLink, Navigate, Route, Routes, useNavigate } from 'react-router-dom';

import { useAuthStore } from './store/useAuthStore';
import { AiCallDashboardPage } from './pages/AiCallDashboardPage';
import { BatchCallPage } from './pages/BatchCallPage';
import { CallHistoryPage } from './pages/CallHistoryPage';
import { CallManagerPage } from './pages/CallManagerPage';
import { LoginPage } from './pages/LoginPage';
import { RetentionPage } from './pages/RetentionPage';
import { ETLPage } from './pages/ETLPage';
import { RetentionTasksPage } from './pages/RetentionTasksPage';
import { ClientScoringPage } from './pages/ClientScoringPage';
import { ActivityDashboardPage } from './pages/admin/ActivityDashboardPage';
import { AuditLogPage } from './pages/admin/AuditLogPage';
import { IntegrationsPage } from './pages/admin/IntegrationsPage';
import { RolesPage } from './pages/admin/RolesPage';
import { UsersPage } from './pages/admin/UsersPage';

interface NavSection {
  title: string;
  adminOnly?: boolean;
  items: { to: string; label: string; permission?: string }[];
}

const NAV_SECTIONS: NavSection[] = [
  {
    title: 'AI Calls',
    items: [
      { to: '/call-manager', label: 'Call Manager', permission: 'call-manager' },
      { to: '/call-history', label: 'Call History', permission: 'call-history' },
      { to: '/call-dashboard', label: 'AI Call Dashboard', permission: 'call-dashboard' },
      { to: '/batch-call', label: 'Batch Call from File', permission: 'batch-call' },
    ],
  },
  {
    title: 'Retention',
    items: [
      { to: '/retention', label: 'Retention Manager', permission: 'retention' },
      { to: '/retention-tasks', label: 'Retention Tasks', permission: 'retention-tasks' },
      { to: '/client-scoring', label: 'Client Scoring', permission: 'client-scoring' },
    ],
  },
  {
    title: 'Administration',
    adminOnly: true,
    items: [
      { to: '/admin/users', label: 'Users' },
      { to: '/admin/roles', label: 'Roles' },
      { to: '/admin/etl', label: 'Data Sync' },
      { to: '/admin/integrations', label: 'Integrations & Config' },
      { to: '/admin/activity', label: 'Activity Dashboard' },
      { to: '/admin/audit-log', label: 'Audit Log' },
    ],
  },
];

const ROUTES: { path: string; title: string; element: ReactNode; adminOnly?: boolean; permission?: string }[] = [
  { path: '/call-manager', title: 'Call Manager', element: <CallManagerPage />, permission: 'call-manager' },
  { path: '/call-history', title: 'Call History', element: <CallHistoryPage />, permission: 'call-history' },
  { path: '/call-dashboard', title: 'AI Call Dashboard', element: <AiCallDashboardPage />, permission: 'call-dashboard' },
  { path: '/batch-call', title: 'Batch Call from File', element: <BatchCallPage />, permission: 'batch-call' },
  { path: '/retention', title: 'Retention Manager', element: <RetentionPage />, permission: 'retention' },
  { path: '/retention-tasks', title: 'Retention Tasks', element: <RetentionTasksPage />, permission: 'retention-tasks' },
  { path: '/client-scoring', title: 'Client Scoring', element: <ClientScoringPage />, permission: 'client-scoring' },
  { path: '/admin/users', title: 'Users', element: <UsersPage />, adminOnly: true },
  { path: '/admin/roles', title: 'Roles', element: <RolesPage />, adminOnly: true },
  { path: '/admin/etl', title: 'Data Sync', element: <ETLPage />, adminOnly: true },
  { path: '/admin/integrations', title: 'Integrations & Configuration', element: <IntegrationsPage />, adminOnly: true },
  { path: '/admin/activity', title: 'Activity Dashboard', element: <ActivityDashboardPage />, adminOnly: true },
  { path: '/admin/audit-log', title: 'Audit Log', element: <AuditLogPage />, adminOnly: true },
];

function ProtectedLayout() {
  const { token, role, permissions, logout } = useAuthStore();
  const navigate = useNavigate();
  const [collapsed, setCollapsed] = useState(() => localStorage.getItem('sidebar_collapsed') === 'true');

  const toggleSidebar = () => setCollapsed((v) => {
    localStorage.setItem('sidebar_collapsed', String(!v));
    return !v;
  });

  if (!token) return <Navigate to="/login" replace />;

  const isAdmin = role === 'admin';

  const visibleSections = NAV_SECTIONS.map((section) => ({
    ...section,
    items: section.items.filter((item) =>
      isAdmin || (!section.adminOnly && item.permission && permissions.includes(item.permission))
    ),
  })).filter((section) => isAdmin || (!section.adminOnly && section.items.length > 0));

  const handleLogout = () => {
    logout();
    navigate('/login', { replace: true });
  };

  return (
    <div className="flex h-screen bg-gray-100 overflow-hidden">
      <aside className={`${collapsed ? 'w-12' : 'w-56'} bg-gray-900 text-white flex flex-col flex-shrink-0 transition-all duration-200 overflow-hidden`}>
        <div className={`px-3 py-5 border-b border-gray-700 flex items-center flex-shrink-0 ${collapsed ? 'justify-center' : 'justify-between'}`}>
          {!collapsed && <span className="text-lg font-bold tracking-tight whitespace-nowrap">Back Office</span>}
          <button
            onClick={toggleSidebar}
            title={collapsed ? 'Expand menu' : 'Collapse menu'}
            className="text-gray-400 hover:text-white transition-colors text-lg leading-none flex-shrink-0"
          >
            {collapsed ? '›' : '‹'}
          </button>
        </div>
        {!collapsed && (
          <>
            <nav className="flex-1 px-3 py-4 space-y-6 overflow-y-auto">
              {visibleSections.map((section) => (
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
            <div className="px-3 py-4 border-t border-gray-700">
              <p className="px-2 text-xs text-gray-400 mb-2 truncate">{useAuthStore.getState().username}</p>
              <button
                onClick={handleLogout}
                className="w-full text-left px-3 py-2 rounded-md text-sm font-medium text-gray-300 hover:bg-gray-700 hover:text-white transition-colors"
              >
                Sign Out
              </button>
            </div>
          </>
        )}
      </aside>

      <div className="flex-1 flex flex-col overflow-hidden">
        <Routes>
          {ROUTES.map(({ path, title, element, adminOnly, permission }) => {
            const hasAccess = isAdmin || (!adminOnly && permission && permissions.includes(permission));
            return (
              <Route
                key={path}
                path={path}
                element={
                  hasAccess ? (
                    <>
                      <header className="bg-white shadow-sm px-6 py-4 flex-shrink-0">
                        <h1 className="text-lg font-semibold text-gray-800">{title}</h1>
                      </header>
                      <main className="flex-1 overflow-y-auto px-6 py-6 space-y-6">
                        {element}
                      </main>
                    </>
                  ) : (
                    <Navigate to="/call-manager" replace />
                  )
                }
              />
            );
          })}
          <Route path="*" element={<Navigate to="/call-manager" replace />} />
        </Routes>
      </div>
    </div>
  );
}

export default function App() {
  return (
    <Routes>
      <Route path="/login" element={<LoginPage />} />
      <Route path="/*" element={<ProtectedLayout />} />
    </Routes>
  );
}
