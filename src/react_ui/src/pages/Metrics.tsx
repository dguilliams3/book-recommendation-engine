import { useEffect, useState } from 'react';
import { BarChart,Bar,XAxis,YAxis,Tooltip,ResponsiveContainer } from 'recharts';
interface Row{label:string;value:number;}
export default function Metrics(){
  const[data,setData]=useState<Row[]>([]);
  useEffect(()=>{
    (async()=>{
      const r = await fetch('/api/metrics/summary'); // backend endpoint TBD
      setData(await r.json());
    })();
  },[]);
  return(
    <>
      <h2>System Metrics</h2>
      <ResponsiveContainer width="100%" height={300}>
        <BarChart data={data}>
          <XAxis dataKey="label"/><YAxis/><Tooltip/>
          <Bar dataKey="value" fill="#00bFFF"/>
        </BarChart>
      </ResponsiveContainer>
    </>
  );
} 