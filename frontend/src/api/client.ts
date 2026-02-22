import axios from 'axios';

import type { CallResponse, ClientDetail, FilterParams } from '../types';

const api = axios.create({
  baseURL: import.meta.env.VITE_API_BASE_URL || '/api',
});

export async function getClients(filters: FilterParams): Promise<ClientDetail[]> {
  // Strip undefined / empty-string values so they don't appear as query params
  const params = Object.fromEntries(
    Object.entries(filters).filter(([, v]) => v !== undefined && v !== '')
  );
  const response = await api.get<ClientDetail[]>('/clients', { params });
  return response.data;
}

export async function initiateCalls(clientIds: string[]): Promise<CallResponse> {
  const response = await api.post<CallResponse>('/calls/initiate', {
    client_ids: clientIds,
  });
  return response.data;
}
