import { useEffect, useState } from 'react';
interface Rec { book_id:string; title:string; blurb:string; }
export default function RecList({studentId}:{studentId:string}) {
  const [recs, setRecs] = useState<Rec[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    if (!studentId) {
      setRecs([]);
      setError(null);
      return;
    }
    setLoading(true);
    setError(null);
    (async () => {
      try {
        const r = await fetch('/api/recommend', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ student_id: studentId })
        });
        if (!r.ok) {
          const msg = await r.text();
          throw new Error(`API error: ${r.status} ${msg}`);
        }
        const j = await r.json();
        setRecs(j.recommendations || []);
      } catch (e: any) {
        setError(e.message || 'Unknown error');
        setRecs([]);
      } finally {
        setLoading(false);
      }
    })();
  }, [studentId]);

  if (loading) return <div>Loading...</div>;
  if (error) return <div style={{color:'red'}}>Error: {error}</div>;
  if (!recs.length) return <div>No recommendations.</div>;
  return <ul>{recs.map(r => <li key={r.book_id}><b>{r.title}</b> — {r.blurb}</li>)}</ul>;
} 