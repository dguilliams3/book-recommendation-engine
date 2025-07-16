import {BrowserRouter,Routes,Route,NavLink} from 'react-router-dom';
import Dashboard from './pages/Dashboard';
import Metrics from './pages/Metrics';
import DataExplorer from './pages/DataExplorer';

export default function App(){
  return(
    <BrowserRouter>
      <nav style={{padding:10}}>
        <NavLink to="/" end style={{marginRight:10}}>Recommendations</NavLink>
        <NavLink to="/metrics" style={{marginRight:10}}>Metrics</NavLink>
        <NavLink to="/data">Data</NavLink>
      </nav>
      <Routes>
        <Route path="/" element={<Dashboard/>}/>
        <Route path="/metrics" element={<Metrics/>}/>
        <Route path="/data" element={<DataExplorer/>}/>
      </Routes>
    </BrowserRouter>
  );
}
