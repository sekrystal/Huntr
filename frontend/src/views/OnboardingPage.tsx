import { useEffect, useState } from "react";
import { useNavigate } from "react-router-dom";
import { getCandidateProfile, type CandidateProfile } from "../lib/api";
import { ProfileEditor } from "./ProfileEditor";

export function OnboardingPage() {
  const navigate = useNavigate();
  const [profile, setProfile] = useState<CandidateProfile | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let active = true;
    getCandidateProfile()
      .then((payload) => {
        if (active) {
          setProfile(payload);
        }
      })
      .catch((err: Error) => {
        if (active) {
          setError(err.message);
        }
      });
    return () => {
      active = false;
    };
  }, []);

  return (
    <>
      <section className="panel onboarding-hero">
        <div className="panel-header">
          <div>
            <p className="eyebrow">Quick setup</p>
            <h3>Point Jorb at the jobs you want</h3>
          </div>
          <p className="panel-copy">
            This onboarding is intentionally skippable. Save a few targeting preferences now, or jump straight into the jobs flow and refine the profile later.
          </p>
        </div>
        <div className="callout-grid">
          <article className="callout-card">
            <span className="detail-label">Fast first value</span>
            <p>One short step gets you into the shortlist. No resume upload or long setup is required to start browsing jobs.</p>
          </article>
          <article className="callout-card">
            <span className="detail-label">Useful search inputs</span>
            <p>Target roles, geography, and work mode feed discovery and ranking immediately, so the shortlist gets sharper without heavy onboarding.</p>
          </article>
        </div>
      </section>
      {error ? <p className="state-copy error-copy">{error}</p> : null}
      {profile ? (
        <ProfileEditor
          description="Set the few preferences that matter most to search quality, then head into jobs. You can reopen Profile anytime to adjust them."
          eyebrow="Optional onboarding"
          onSaved={(saved) => {
            setProfile(saved);
            navigate("/jobs");
          }}
          onSkip={() => navigate("/jobs")}
          profile={profile}
          skipLabel="Skip to jobs"
          submitLabel="Save and open jobs"
          successMessage="Profile preferences saved. Opening jobs."
          title="Start with lightweight preferences"
        />
      ) : (
        <section className="panel">
          <p className="state-copy">Loading candidate profile from FastAPI.</p>
        </section>
      )}
    </>
  );
}
