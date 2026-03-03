import { useEffect, useState } from "react";

import { getLiveSessions } from "../api/client";
import { LiveSessionItem } from "../api/types";

interface Props {
  refreshTick: number;
}

export default function LiveSessionsTable({ refreshTick }: Props) {
  const [items, setItems] = useState<LiveSessionItem[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState<boolean>(true);

  useEffect(() => {
    let canceled = false;

    const load = async () => {
      try {
        const rows = await getLiveSessions();
        if (!canceled) {
          setItems(rows);
          setError(null);
        }
      } catch (err) {
        if (!canceled) {
          setError(err instanceof Error ? err.message : "Failed to load sessions");
        }
      } finally {
        if (!canceled) {
          setLoading(false);
        }
      }
    };

    load();
    const id = window.setInterval(load, 15000);

    return () => {
      canceled = true;
      window.clearInterval(id);
    };
  }, [refreshTick]);

  return (
    <section className="card">
      <h2>Live Sessions</h2>
      {loading && <p>Loading sessions...</p>}
      {error && <p className="message">{error}</p>}
      {!loading && !error && items.length === 0 && <p>No sessions found.</p>}
      {items.length > 0 && (
        <div className="table-wrap">
          <table>
            <thead>
              <tr>
                <th>ID</th>
                <th>Creator</th>
                <th>Title</th>
                <th>URL</th>
                <th>Processed</th>
              </tr>
            </thead>
            <tbody>
              {items.map((item) => (
                <tr key={item.video_id}>
                  <td>{item.video_id}</td>
                  <td>{item.creator_name}</td>
                  <td>{item.title}</td>
                  <td>
                    <a href={item.url} target="_blank" rel="noreferrer">
                      {item.url}
                    </a>
                  </td>
                  <td>{item.processed ? "yes" : "no"}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </section>
  );
}
