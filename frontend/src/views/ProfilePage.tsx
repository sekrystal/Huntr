import { useEffect, useState } from "react";
import { getCandidateProfile, type CandidateProfile } from "../lib/api";
import { ProfileEditor } from "./ProfileEditor";

export function ProfilePage() {
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
      {error ? <p className="state-copy error-copy">{error}</p> : null}
      {profile ? (
        <>
          <ProfileEditor
            description="Keep setup light: set the roles, geography, and work mode that should shape search and ranking. Resume upload can still deepen the profile later."
            eyebrow="Candidate profile"
            onSaved={setProfile}
            profile={profile}
            submitLabel="Save profile preferences"
            successMessage="Profile preferences updated for the shortlist."
            title="Lightweight search preferences"
          />
          <section className="panel">
            <div className="panel-header">
              <div>
                <p className="eyebrow">Current profile signals</p>
                <h3>What search is using right now</h3>
              </div>
              <p className="panel-copy">
                These values already feed the FastAPI profile payload and stay visible without surfacing internal diagnostics in the product shell.
              </p>
            </div>
            <div className="profile-grid">
              <article>
                <h4>Target roles</h4>
                <p>{profile.targetRoles.join(", ") || "No roles configured."}</p>
              </article>
              <article>
                <h4>Preferred geography</h4>
                <p>{profile.targetLocations.join(", ") || "No locations configured."}</p>
              </article>
              <article>
                <h4>Work mode</h4>
                <p>{profile.workModePreference || "No preference configured."}</p>
              </article>
              <article>
                <h4>Preferred domains</h4>
                <p>{profile.preferredDomains.join(", ") || "No domains configured."}</p>
              </article>
            </div>
          </section>
        </>
      ) : (
        <section className="panel">
          <p className="state-copy">Loading candidate profile from FastAPI.</p>
        </section>
      )}
    </>
  );
}
