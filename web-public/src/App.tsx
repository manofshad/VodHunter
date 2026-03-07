import SearchPage from "./components/SearchPage";

export default function App() {
  return (
    <div className="app-shell">
      <header className="navbar">
        <div className="navbar-inner">
          <div className="brand" aria-label="VodHunter">
            <span className="brand-primary">Vod</span>
            <span className="brand-accent">Hunter</span>
          </div>
        </div>
      </header>
      <SearchPage />
    </div>
  );
}
