import { create } from 'zustand';
import { persist } from 'zustand/middleware';

interface AuthState {
  token: string | null;
  username: string | null;
  role: string | null;
  permissions: string[];
  setAuth: (token: string, username: string, role: string, permissions: string[]) => void;
  logout: () => void;
  isAuthenticated: () => boolean;
}

export const useAuthStore = create<AuthState>()(
  persist(
    (set, get) => ({
      token: null,
      username: null,
      role: null,
      permissions: [],
      setAuth: (token, username, role, permissions) =>
        set({ token, username, role, permissions }),
      logout: () => set({ token: null, username: null, role: null, permissions: [] }),
      isAuthenticated: () => !!get().token,
    }),
    { name: 'auth' }
  )
);
