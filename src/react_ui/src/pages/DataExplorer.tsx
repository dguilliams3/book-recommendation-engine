import { useEffect,useState } from 'react';
import {createColumnHelper,useReactTable,getCoreRowModel,flexRender}
  from '@tanstack/react-table';
export default function DataExplorer(){
  const[data,setData]=useState<Record<string,any>[]>([]);
  const[columns,setColumns]=useState<any[]>([]);
  useEffect(()=>{
    (async()=>{
      const r = await fetch('/api/books?limit=100'); // backend endpoint TBD
      const j = await r.json(); const rows=j.rows;
      setData(rows);
      if(rows.length){
         const ch=createColumnHelper<Record<string,any>>();
         setColumns(Object.keys(rows[0]).map(k=>ch.accessor(k,{header:k.toUpperCase()})));
      }
    })();
  },[]);
  const table = useReactTable({data,columns,getCoreRowModel:getCoreRowModel()});
  return(
    <>
      <h2>Catalog (first 100)</h2>
      <table className="table is-striped">
        <thead>{table.getHeaderGroups().map(hg=><tr key={hg.id}>
          {hg.headers.map(h=><th key={h.id}>{flexRender(h.column.columnDef.header,h.getContext())}</th>)}
        </tr>)}</thead>
        <tbody>{table.getRowModel().rows.map(r=><tr key={r.id}>
          {r.getVisibleCells().map(c=><td key={c.id}>{flexRender(c.column.columnDef.cell,c.getContext())}</td>)}
        </tr>)}</tbody>
      </table>
    </>
  );
} 