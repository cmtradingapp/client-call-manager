import axios from 'axios';

const api = axios.create({
  baseURL: import.meta.env.VITE_API_BASE_URL || '/api',
});

export async function login(username: string, password: string) {
  const res = await api.post('/auth/login', { username, password });
  return res.data as {
    access_token: string;
    username: string;
    role: string;
    permissions: string[];
  };
}
