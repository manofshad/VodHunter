import SearchPage from "./components/SearchPage";

export default function App() {
  return (
    <div className="app-shell">
      <header className="navbar">
        <div className="navbar-inner">
          <div className="brand">Vod Hunter</div>
        </div>
      </header>
      <SearchPage />
    </div>
  );
}
