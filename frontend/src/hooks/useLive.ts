import {useEffect,useState} from 'react';
import type {Live} from '../types';
export function useLive(){
 const [live,setLive]=useState<Live>({metrics:{state:'STOPPED'},logs:[],history:[],generator:{state:'IDLE'},nodes:[]});
 const [connected,setConnected]=useState(false);
 useEffect(()=>{
  let disposed=false;let socket:WebSocket;let timer:ReturnType<typeof setTimeout>;let session:string|undefined;
  const open=()=>{
   socket=new WebSocket(`${location.protocol==='https:'?'wss':'ws'}://${location.host}/ws/live`);
   socket.onopen=()=>setConnected(true);
   socket.onmessage=e=>{
    const data=JSON.parse(e.data) as Live;
    const changed=session!==data.metrics.session_id;session=data.metrics.session_id;
    setLive(prev=>({...data,logs:[...(changed?[]:prev.logs),...data.logs].slice(-1000)}));
   };
   socket.onclose=()=>{setConnected(false);if(!disposed)timer=setTimeout(open,2000)};
   socket.onerror=()=>socket.close();
  };open();
  return()=>{disposed=true;clearTimeout(timer);socket.close()};
 },[]);
 return {live,connected};
}
