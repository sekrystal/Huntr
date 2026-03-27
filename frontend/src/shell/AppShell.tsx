import { NavLink, Outlet } from "react-router-dom";

const navigation = [
  { to: "/jobs", label: "Jobs", detail: "Default shortlist" },
  { to: "/saved", label: "Saved", detail: "Follow-up queue" },
  { to: "/applied", label: "Applied", detail: "Tracker" },
  { to: "/profile", label: "Profile", detail: "Candidate context" },
];

export function AppShell() {
  return (
    <div className="app-shell">
      <aside className="sidebar">
        <div className="sidebar-brand">
          <p className="eyebrow">JORB</p>
          <h1>Opportunity Scout</h1>
          <p className="sidebar-copy">Primary product shell for reviewing surfaced opportunities and managing tracker state.</p>
        </div>
        <nav className="nav-list" aria-label="Primary">
          {navigation.map((item) => (
            <NavLink
              className={({ isActive }) => `nav-item${isActive ? " is-active" : ""}`}
              key={item.to}
              to={item.to}
            >
              <span>{item.label}</span>
              <small>{item.detail}</small>
            </NavLink>
          ))}
        </nav>
        <div className="sidebar-status">
          <p className="status-label">System Status</p>
          <p className="status-copy">FastAPI-backed product routes stay primary here. Streamlit is reserved for internal validation and operator workflows.</p>
          <div className="sidebar-links">
            <NavLink className="sidebar-link" to="/welcome">
              Quick setup
            </NavLink>
            <a className="sidebar-link" href="http://127.0.0.1:8000/docs" target="_blank" rel="noreferrer">
              API docs
            </a>
            <NavLink className="sidebar-link" to="/validation-harness">
              Internal Harness
            </NavLink>
          </div>
        </div>
      </aside>
      <main className="content">
        <Outlet />
      </main>
    </div>
  );
}
