import { type FormEvent, useEffect, useState } from "react";
import { type CandidateProfile, saveCandidateProfile } from "../lib/api";

export const ONBOARDING_STATE_KEY = "jorb-onboarding-state";
export type OnboardingState = "pending" | "skipped" | "completed";

type ProfileEditorProps = {
  profile: CandidateProfile;
  title: string;
  eyebrow: string;
  description: string;
  submitLabel: string;
  successMessage: string;
  onSaved?: (profile: CandidateProfile) => void;
  onSkip?: () => void;
  skipLabel?: string;
};

type FormState = {
  targetRoles: string;
  targetLocations: string;
  workModePreference: string;
};

function parseListInput(value: string): string[] {
  return value
    .split(/\n|,/)
    .map((item) => item.trim())
    .filter(Boolean);
}

function toFormState(profile: CandidateProfile): FormState {
  return {
    targetRoles: profile.targetRoles.join("\n"),
    targetLocations: profile.targetLocations.join("\n"),
    workModePreference: profile.workModePreference || "unspecified",
  };
}

export function readOnboardingState(): OnboardingState {
  if (typeof window === "undefined") {
    return "pending";
  }
  const stored = window.localStorage.getItem(ONBOARDING_STATE_KEY);
  return stored === "skipped" || stored === "completed" ? stored : "pending";
}

export function writeOnboardingState(next: OnboardingState) {
  if (typeof window === "undefined") {
    return;
  }
  window.localStorage.setItem(ONBOARDING_STATE_KEY, next);
}

export function ProfileEditor({
  profile,
  title,
  eyebrow,
  description,
  submitLabel,
  successMessage,
  onSaved,
  onSkip,
  skipLabel = "Skip for now",
}: ProfileEditorProps) {
  const [form, setForm] = useState<FormState>(() => toFormState(profile));
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [success, setSuccess] = useState<string | null>(null);

  useEffect(() => {
    setForm(toFormState(profile));
  }, [profile]);

  async function handleSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setSaving(true);
    setError(null);
    setSuccess(null);

    try {
      const saved = await saveCandidateProfile({
        ...profile.raw,
        target_roles_json: parseListInput(form.targetRoles),
        preferred_locations_json: parseListInput(form.targetLocations),
        work_mode_preference: form.workModePreference,
      });
      writeOnboardingState("completed");
      setSuccess(successMessage);
      onSaved?.(saved);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Unable to save profile settings.");
    } finally {
      setSaving(false);
    }
  }

  return (
    <section className="panel">
      <div className="panel-header">
        <div>
          <p className="eyebrow">{eyebrow}</p>
          <h3>{title}</h3>
        </div>
        <p className="panel-copy">{description}</p>
      </div>
      <div className="callout-grid">
        <article className="callout-card">
          <span className="detail-label">Target roles</span>
          <p>Feeds search queries and role-fit ranking so the shortlist reflects the jobs you actually want.</p>
        </article>
        <article className="callout-card">
          <span className="detail-label">Geography + work mode</span>
          <p>Helps suppress mismatched locations while keeping remote, hybrid, and onsite preferences explicit.</p>
        </article>
      </div>
      <form className="profile-form" onSubmit={handleSubmit}>
        <label className="profile-field">
          <span className="field-label">Target roles</span>
          <textarea
            name="targetRoles"
            onChange={(event) => setForm((current) => ({ ...current, targetRoles: event.target.value }))}
            placeholder={"Chief of Staff\nFounding Operations Lead"}
            rows={4}
            value={form.targetRoles}
          />
          <small>One per line or comma-separated.</small>
        </label>
        <label className="profile-field">
          <span className="field-label">Preferred geography</span>
          <textarea
            name="targetLocations"
            onChange={(event) => setForm((current) => ({ ...current, targetLocations: event.target.value }))}
            placeholder={"San Francisco\nRemote US"}
            rows={4}
            value={form.targetLocations}
          />
          <small>Use the places or remote scopes you want search and ranking to prioritize.</small>
        </label>
        <label className="profile-field">
          <span className="field-label">Work mode</span>
          <select
            name="workModePreference"
            onChange={(event) => setForm((current) => ({ ...current, workModePreference: event.target.value }))}
            value={form.workModePreference}
          >
            <option value="unspecified">No preference yet</option>
            <option value="remote">Remote</option>
            <option value="hybrid">Hybrid</option>
            <option value="onsite">Onsite</option>
          </select>
          <small>Keep this lightweight. You can refine the profile later without blocking the jobs flow.</small>
        </label>
        {error ? <p className="state-copy error-copy">{error}</p> : null}
        {success ? <p className="state-copy success-copy">{success}</p> : null}
        <div className="profile-actions">
          <button className="primary-button" disabled={saving} type="submit">
            {saving ? "Saving..." : submitLabel}
          </button>
          {onSkip ? (
            <button
              className="secondary-button"
              onClick={() => {
                writeOnboardingState("skipped");
                onSkip();
              }}
              type="button"
            >
              {skipLabel}
            </button>
          ) : null}
        </div>
      </form>
    </section>
  );
}
