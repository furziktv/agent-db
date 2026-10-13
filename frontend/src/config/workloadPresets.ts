import type {Workload} from '../types';
export const MODES=[{value:'NORMAL',label:'Обычная нагрузка'},{value:'INTEGRITY',label:'Проверка сохранности'},{value:'MIXED',label:'Нагрузка + проверка сохранности'}] as const;
const balanced:Partial<Workload>={select_percent:70,insert_percent:20,update_percent:10,simple_percent:25,join_percent:20,aggregate_percent:10,relational_insert_percent:15,relational_update_percent:15,transaction_percent:15};
interface Preset {id:string;label:string;description:string;relationalOnly?:boolean;values:Partial<Workload>}
export const PRESETS:Preset[]=[
 {id:'BALANCED',label:'Сбалансированный',description:'Обычная смешанная нагрузка, 10 workers, 100 RPS.',values:{...balanced,mode:'NORMAL',workers:10,rps:100,min_payload:100,max_payload:10000}},
 {id:'READ',label:'Чтение',description:'Только SELECT, включая JOIN и агрегаты в связанной БД.',values:{mode:'NORMAL',workers:10,rps:100,min_payload:100,max_payload:10000,select_percent:100,insert_percent:0,update_percent:0,simple_percent:60,join_percent:30,aggregate_percent:10,relational_insert_percent:0,relational_update_percent:0,transaction_percent:0}},
 {id:'WRITE',label:'Запись',description:'Преимущественно INSERT и UPDATE.',values:{mode:'NORMAL',workers:10,rps:100,min_payload:100,max_payload:10000,select_percent:20,insert_percent:50,update_percent:30,simple_percent:10,join_percent:5,aggregate_percent:5,relational_insert_percent:35,relational_update_percent:25,transaction_percent:20}},
 {id:'TRANSACTIONS',label:'Транзакции',description:'60% транзакционных сценариев; требуется связанная тестовая БД.',relationalOnly:true,values:{...balanced,mode:'NORMAL',workers:10,rps:100,min_payload:100,max_payload:10000,simple_percent:10,join_percent:10,aggregate_percent:0,relational_insert_percent:10,relational_update_percent:10,transaction_percent:60}},
 {id:'INTEGRITY',label:'Сохранность данных',description:'Каждый цикл: INSERT, чтение, UPDATE и повторное чтение.',values:{...balanced,mode:'INTEGRITY',workers:4,rps:100,min_payload:50,max_payload:1000}},
 {id:'COMBINED',label:'Нагрузка и сохранность',description:'80% обычных операций, 20% проверочных циклов.',values:{...balanced,mode:'MIXED',integrity_percent:20,workers:10,rps:100,min_payload:100,max_payload:10000}},
 {id:'FAILOVER',label:'Проверка failover',description:'50% проверочных циклов, небольшие payload, refresh каждые 5 операций.',values:{...balanced,mode:'MIXED',integrity_percent:50,workers:4,rps:50,min_payload:50,max_payload:1000,connection_refresh_every:5}},
];
export function applyPreset(w:Workload,id:string,limits:Record<string,number>):Workload {
 const p=PRESETS.find(p=>p.id===id);if(!p)return w;
 const next={...w,...p.values,duration_seconds:0,max_requests:0,delay_ms:0,rate_mode:'PACED',complexity:'RANDOM',index_mode:'INDEXED',payload_distribution:'UNIFORM',query_timeout:10,reconnect_timeout:10,slow_threshold_ms:500};
 next.integrity_test=next.mode==='INTEGRITY';
 next.workers=Math.min(next.workers,limits.MAX_WORKERS);
 next.rps=Math.min(next.rps,limits.MAX_RPS);
 next.max_payload=Math.min(next.max_payload,limits.MAX_PAYLOAD_BYTES);
 next.min_payload=Math.min(next.min_payload,next.max_payload);
 next.pool_max_size=Math.max(1,Math.min(next.workers,limits.MAX_CONNECTIONS-2));
 next.pool_min_size=1;
 return next;
}
