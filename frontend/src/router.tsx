import { Navigate, createBrowserRouter } from "react-router-dom";
import { AppShell } from "./shell/AppShell";
import { JobsPage, SavedPage, AppliedPage } from "./views/LeadPages";
import { ProfilePage } from "./views/ProfilePage";
import { ValidationHarnessPage } from "./views/ValidationHarnessPage";
import { OnboardingPage } from "./views/OnboardingPage";
import { readOnboardingState } from "./views/ProfileEditor";

function IndexRedirect() {
  if (readOnboardingState() === "pending") {
    return <Navigate to="/welcome" replace />;
  }
  return <Navigate to="/jobs" replace />;
}

export const router = createBrowserRouter([
  {
    path: "/",
    element: <AppShell />,
    children: [
      { index: true, element: <IndexRedirect /> },
      { path: "welcome", element: <OnboardingPage /> },
      { path: "jobs", element: <JobsPage /> },
      { path: "saved", element: <SavedPage /> },
      { path: "applied", element: <AppliedPage /> },
      { path: "profile", element: <ProfilePage /> },
      { path: "validation-harness", element: <ValidationHarnessPage /> },
    ],
  },
]);
