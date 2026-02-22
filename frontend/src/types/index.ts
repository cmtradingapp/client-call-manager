export type ClientStatus = 'active' | 'inactive' | 'pending';

export type CallStatusType = 'idle' | 'calling' | 'initiated' | 'failed';

export interface FilterParams {
  date_from?: string;
  date_to?: string;
  status?: ClientStatus;
  region?: string;
  custom_field?: string;
}

export interface ClientDetail {
  client_id: string;
  name: string;
  status: string;
  region?: string;
  created_at?: string;
  phone_number: string;
  email?: string;
  account_manager?: string;
}

export interface ClientCallResult {
  client_id: string;
  status: 'initiated' | 'failed';
  conversation_id?: string;
  error?: string;
}

export interface CallResponse {
  results: ClientCallResult[];
}
