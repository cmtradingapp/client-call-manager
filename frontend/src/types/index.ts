export type CallStatusType = 'idle' | 'calling' | 'initiated' | 'failed';

export interface Country {
  name: string;
  iso2code: string;
}

export interface SalesStatus {
  id: number;
  value: string;
}

export interface FilterParams {
  date_from?: string;
  date_to?: string;
  sales_status?: number;
  region?: string;
  custom_field?: string;
  sales_client_potential?: number;
  sales_client_potential_op?: string;
  language?: string;
  live?: string;
  ftd?: string;
}

export interface ClientDetail {
  client_id: string;
  name: string;
  status: string;
  region?: string;
  created_at?: string;
  phone_number?: string;
  email?: string;
  account_manager?: string;
  sales_client_potential?: number;
  language?: string;
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

export interface CallHistoryRecord {
  id: number;
  client_id: string;
  client_name?: string;
  phone_number?: string;
  conversation_id?: string;
  status: 'initiated' | 'failed';
  called_at: string;
  error?: string;
  agent_id?: string;
}
