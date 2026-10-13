import {useCallback,useEffect,useState} from 'react';
import {Activity,Settings,Network,FileBarChart,ScrollText,Database,Zap,ExternalLink} from 'lucide-react';
import {api} from './api/client';
import {useLive} from './hooks/useLive';
import {Dashboard} from './pages/Dashboard';
import {Configuration} from './pages/Configuration';
import {Nodes} from './pages/Nodes';
import {Reports} from './pages/Reports';
import {Logs} from './pages/Logs';
import {TestDatabases} from './pages/TestDatabases';
import type {Config,ManagedDatabase} from './types';
const navigation=[['Dashboard',Activity],['Configuration',Settings],['Test Databases',Database],['Nodes',Network],['Reports',FileBarChart],['Logs',ScrollText]] as const;
export default function App(){const {live,connected}=useLive();const [page,setPage]=useState('Dashboard');const [config,setConfig]=useState<Config>();const [limits,setLimits]=useState<Record<string,number>>({});const [databases,setDatabases]=useState<ManagedDatabase[]>([]);const [toast,setToast]=useState<{text:string;error:boolean}>();
 const notify=useCallback((text:string,error=false)=>setToast({text,error}),[]);
 const reload=useCallback(async()=>{const [c,d]=await Promise.all([api<{config:Config;limits:Record<string,number>}>('/config'),api<ManagedDatabase[]>('/databases')]);setConfig(c.config);setLimits(c.limits);setDatabases(d)},[]);
 useEffect(()=>{reload().catch(e=>notify(e.message,true))},[reload,notify]);
 useEffect(()=>{if(['READY','FAILED','CANCELLED'].includes(live.generator.state))reload().catch(e=>notify(e.message,true))},[live.generator.state,reload,notify]);
 useEffect(()=>{if(toast){const t=setTimeout(()=>setToast(undefined),7000);return()=>clearTimeout(t)}},[toast]);
 const locked=live.metrics.state!=='STOPPED';
 return <div className="app"><aside className="sidebar"><div className="brand"><div className="brand-icon"><Zap size={22}/></div><div><strong>MySQL</strong><span>WORKLOAD AGENT</span></div></div><div className="nav-label">WORKSPACE</div><nav>{navigation.map(([name,Icon])=><button className={page===name?'active':''} key={name} onClick={()=>setPage(name)}><Icon size={19}/>{name}{name==='Logs'&&Number(live.metrics.failed)>0&&<i>{String(live.metrics.failed)}</i>}</button>)}</nav><div className="sidebar-bottom"><span className={`live-dot ${connected?'':'offline'}`}/>{connected?'Live telemetry connected':'Reconnecting telemetry'}<small>LOCAL INSTANCE · v1.0</small><a href="http://localhost:8000/docs" target="_blank" rel="noreferrer">API documentation <ExternalLink size={12}/></a></div></aside><div className="main"><header><div><span className="muted">Workspace</span><span className="crumb">/</span>{page}</div><div className="header-right"><span className="local-pill">LOCAL</span><span className="muted">MySQL Workload Agent</span></div></header><main>{page==='Dashboard'&&<Dashboard live={live} config={config} databases={databases} limits={limits} reload={reload} notify={notify}/>} {page==='Configuration'&&<Configuration config={config} limits={limits} reload={reload} notify={notify} locked={locked}/>} {page==='Test Databases'&&<TestDatabases databases={databases} progress={live.generator} config={config} verified={!!live.metrics.verified} limits={limits} reload={reload} notify={notify} locked={locked}/>} {page==='Nodes'&&<Nodes nodes={live.nodes} config={config} notify={notify}/>} {page==='Reports'&&<Reports notify={notify}/>} {page==='Logs'&&<Logs logs={live.logs}/>}</main></div>{toast&&<div role="status" className={`toast ${toast.error?'error-toast':''}`} onClick={()=>setToast(undefined)}>{toast.text}</div>}</div>
}
