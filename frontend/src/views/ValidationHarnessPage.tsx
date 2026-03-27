export function ValidationHarnessPage() {
  return (
    <section className="panel">
      <div className="panel-header">
        <div>
          <p className="eyebrow">Secondary shell</p>
          <h3>Streamlit is now the internal validation and operator harness</h3>
        </div>
        <p className="panel-copy">
          The JS workbench is the product path. Streamlit stays available for debugging, validation, and operator-only surfaces that should not be part of the user-facing shell.
        </p>
      </div>
      <div className="callout-grid">
        <article className="callout-card">
          <h4>Use the JS shell for</h4>
          <p>Jobs, saved state, applied tracking, profile editing, onboarding, and the default product route hierarchy.</p>
        </article>
        <article className="callout-card">
          <h4>Use Streamlit for internal work</h4>
          <p>Agent activity, investigations, learning, autonomy ops, diagnostics, and other operator-only surfaces.</p>
        </article>
      </div>
      <a className="primary-link" href="http://127.0.0.1:8500" target="_blank" rel="noreferrer">
        Open internal Streamlit harness
      </a>
    </section>
  );
}
