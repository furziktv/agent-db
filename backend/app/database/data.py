import random
import uuid
import hashlib
import json
from datetime import datetime, timedelta

FIRST = ('Alex','Sam','Robin','Taylor','Morgan','Jordan')
LAST = ('Example','Synthetic','Sample','Demo')

async def row_values(table: str, cur, rng: random.Random, text: str, refs=None):
    async def ref(name):
        if refs is not None:
            return rng.choice(refs[name])
        # Index seek avoids ORDER BY RAND and tolerates ID gaps.
        await cur.execute(f'SELECT MAX(id) FROM `{name}`')
        maximum = (await cur.fetchone())[0]
        if not maximum: raise ValueError('Parent table is empty: '+name)
        await cur.execute(f'SELECT id FROM `{name}` WHERE id >= %s ORDER BY id LIMIT 1',(rng.randint(1,maximum),))
        return (await cur.fetchone())[0]
    token = uuid.uuid4().hex
    price = round(rng.uniform(1,1000),2)
    if table == 'users': return (f'user_{token}',f'{token}@example.invalid',rng.choice(FIRST),rng.choice(LAST),rng.choice(('active','inactive')))
    if table == 'user_profiles': return (await ref('users'),'+0-000-'+str(rng.randint(100000,999999)),'1990-01-01',text,json.dumps({'theme':rng.choice(('dark','light'))}))
    if table == 'addresses': return (await ref('users'),rng.choice(('Testland','Examplestan')),rng.choice(('Demo City','Sample Town')),f'{rng.randint(1,999)} Synthetic Street','00000','shipping')
    if table == 'categories': return (None,'Category '+token[:8],text)
    if table == 'products': return (await ref('categories'),'SKU-'+token,'Product '+token[:10],text,price,'active')
    if table == 'product_inventory': return (await ref('products'),'WH-'+str(rng.randint(1,9)),rng.randint(100,10000),0)
    if table == 'suppliers': return ('Supplier '+token[:8],token+'@example.invalid',text)
    if table == 'product_suppliers': return (await ref('products'),await ref('suppliers'),price)
    if table == 'orders':
        address = await ref('addresses')
        await cur.execute('SELECT user_id FROM addresses WHERE id=%s',(address,))
        user = (await cur.fetchone())[0]
        return (user,address,'new',0)
    if table == 'order_items': return (await ref('orders'),await ref('products'),rng.randint(1,5),price)
    if table == 'payments':
        order = await ref('orders')
        await cur.execute('SELECT total_amount FROM orders WHERE id=%s',(order,))
        return (order,'test-card',(await cur.fetchone())[0],'pending','TEST-'+token)
    if table == 'shipments': return (await ref('orders'),'TEST-'+token,'Demo Carrier','pending',None,None)
    if table == 'reviews': return (await ref('users'),await ref('products'),rng.randint(1,5),'Synthetic review',text)
    if table == 'sessions': return (await ref('users'),hashlib.sha256(token.encode()).hexdigest(),f'192.0.2.{rng.randint(1,254)}','WorkloadAgent/1.0',datetime.now()+timedelta(days=1))
    if table == 'audit_events': return (await ref('users'),None,'synthetic',text)
    raise ValueError('Unknown managed table')
