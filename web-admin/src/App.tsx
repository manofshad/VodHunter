import { useMemo } from "react";

import LiveMonitorCard from "./components/LiveMonitorCard";
import LiveSessionsTable from "./components/LiveSessionsTable";
import SearchCard from "./components/SearchCard";
import { useLiveStatus } from "./hooks/useLiveStatus";

export default function App() {
  const { status, loading, error, refresh } = useLiveStatus(2500);
  const refreshTick = useMemo(() => Date.now(), [status.state, status.current_video_id]);

  return (
    <main className="container">
      <header>
        <h1>VodHunter Dashboard</h1>
        {loading ? <p>Loading live status...</p> : <p>Monitor and search workflows</p>}
        {error && <p className="message">{error}</p>}
      </header>

      <LiveMonitorCard status={status} onRefresh={refresh} />
      <SearchCard liveStatus={status} />
      <LiveSessionsTable refreshTick={refreshTick} />
    </main>
  );
}
