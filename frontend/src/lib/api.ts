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

export type CandidateProfilePayload = {
  profile_schema_version: string;
  name: string;
  raw_resume_text: string;
  extracted_summary_json: Record<string, unknown>;
  preferred_titles_json: string[];
  adjacent_titles_json: string[];
  excluded_titles_json: string[];
  preferred_domains_json: string[];
  excluded_companies_json: string[];
  preferred_locations_json: string[];
  target_roles_json: string[];
  work_mode_preference: string;
  confirmed_skills_json: string[];
  competencies_json: string[];
  explicit_preferences_json: string[];
  seniority_guess?: string | null;
  stage_preferences_json: string[];
  core_titles_json: string[];
  excluded_keywords_json: string[];
  min_seniority_band: string;
  max_seniority_band: string;
  stretch_role_families_json: string[];
  minimum_fit_threshold: number;
  structured_profile_json?: Record<string, unknown> | null;
};

export type CandidateProfile = {
  name: string;
  targetRoles: string[];
  targetLocations: string[];
  workModePreference: string;
  preferredDomains: string[];
  focusKeywords: string[];
  notes?: string | null;
  raw: CandidateProfilePayload;
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

function toCandidateProfile(payload: CandidateProfilePayload): CandidateProfile {
  const summary = payload.extracted_summary_json ?? {};
  const summaryText = typeof summary.summary === "string" ? summary.summary : null;

  return {
    name: payload.name,
    targetRoles: payload.target_roles_json ?? [],
    targetLocations: payload.preferred_locations_json ?? [],
    workModePreference: payload.work_mode_preference ?? "unspecified",
    preferredDomains: payload.preferred_domains_json ?? [],
    focusKeywords: payload.confirmed_skills_json?.length ? payload.confirmed_skills_json : payload.competencies_json ?? [],
    notes: summaryText,
    raw: payload,
  };
}

export async function getCandidateProfile(): Promise<CandidateProfile> {
  const payload = await requestJson<CandidateProfilePayload>("/candidate-profile");
  return toCandidateProfile(payload);
}

export async function saveCandidateProfile(payload: CandidateProfilePayload): Promise<CandidateProfile> {
  const saved = await requestJson<CandidateProfilePayload>("/candidate-profile", {
    method: "POST",
    body: JSON.stringify(payload),
  });
  return toCandidateProfile(saved);
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
