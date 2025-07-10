import { useState } from 'react';
import RecList from '../components/RecList';

export default function Dashboard() {
  const [input, setInput] = useState('');
  const [studentId, setStudentId] = useState('');

  const commit = () => setStudentId(input.trim());

  return (
    <>
      <h2>Book Recommendations</h2>
      <input
        placeholder="Student ID (e.g. S100)"
        value={input}
        onChange={e => setInput(e.target.value)}
        onBlur={commit}
        onKeyDown={e => { if (e.key === 'Enter') commit(); }}
        style={{marginRight:8}}
      />
      <RecList studentId={studentId}/>
    </>
  );
} 