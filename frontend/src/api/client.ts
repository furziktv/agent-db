export async function api<T>(path:string,body?:unknown,method?:string):Promise<T> {
  const res=await fetch('/api'+path,{method:method??(body===undefined?'GET':'POST'),headers:body===undefined?{}:{'Content-Type':'application/json'},body:body===undefined?undefined:JSON.stringify(body)});
  const data=await res.json();
  if(!res.ok) throw new Error(typeof data.detail==='string'?data.detail:'Некорректные параметры запроса');
  return data as T;
}
