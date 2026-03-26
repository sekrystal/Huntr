export type Lead = {
  id: number;
  company_name: string;
  primary_title: string;
  lead_type: string;
  url?: string | null;
  source_type: string;
  listing_status?: string | null;
  first_published_at?: string | null;
  discovered_at?: string | null;
  last_seen_at?: string | null;
  updated_at?: string | null;
  freshness_hours?: number | null;
  freshness_days?: number | null;
  posted_at?: string | null;
  freshness_label: string;
  rank_label: string;
  title_fit_label: string;
  qualification_fit_label: string;
  confidence_label: string;
  source_provenance?: string | null;
  source_lineage?: string | null;
  discovery_source?: string | null;
  saved?: boolean;
  applied?: boolean;
  date_saved?: string | null;
  date_applied?: string | null;
  application_notes?: string | null;
  application_updated_at?: string | null;
  next_action?: string | null;
  follow_up_due?: boolean;
  current_status?: string | null;
  source_platform?: string | null;
  source_url?: string | null;
  explanation?: string | null;
  surfaced_at?: string | null;
  hidden: boolean;
  score_breakdown_json: Record<string, unknown>;
  evidence_json: Record<string, unknown>;
};

export type CandidateProfile = {
  target_titles: string[];
  target_locations: string[];
  preferred_domains: string[];
  focus_keywords: string[];
  excluded_keywords: string[];
  notes?: string | null;
};

type LeadsResponse = {
  items: Lead[];
};

const API_BASE_URL = (import.meta.env.VITE_API_BASE_URL as string | undefined) ?? "/api";

async function requestJson<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(`${API_BASE_URL}${path}`, {
    headers: {
      "Content-Type": "application/json",
      ...(init?.headers ?? {}),
    },
    ...init,
  });
  if (!response.ok) {
    throw new Error(`Request failed for ${path}: ${response.status}`);
  }
  return (await response.json()) as T;
}

export async function getLeads(params: Record<string, string | boolean | number | undefined> = {}): Promise<Lead[]> {
  const searchParams = new URLSearchParams();
  Object.entries(params).forEach(([key, value]) => {
    if (value === undefined) {
      return;
    }
    searchParams.set(key, String(value));
  });
  const query = searchParams.toString();
  const payload = await requestJson<LeadsResponse>(`/opportunities${query ? `?${query}` : ""}`);
  return payload.items;
}

export function getCandidateProfile(): Promise<CandidateProfile> {
  return requestJson<CandidateProfile>("/candidate-profile");
}

export function setApplicationStatus(payload: {
  lead_id: number;
  current_status: string;
  notes?: string;
  date_applied?: string;
}): Promise<{ status: string; lead_id: number; current_status: string }> {
  return requestJson("/applications/status", {
    method: "POST",
    body: JSON.stringify(payload),
  });
}
