from dataclasses import dataclass
from app.database.schema import NAMES

@dataclass(frozen=True)
class Template:
    name: str
    kind: str
    tables: tuple[str,...]
    sql: str = ''
    complexity: str = 'LOW'

TEMPLATES = []
for t in NAMES:
    TEMPLATES.extend([
        Template('simple_'+t,'simple',(t,),f'SELECT * FROM `{t}` WHERE id >= %s ORDER BY id LIMIT %s'),
        Template('range_'+t,'simple',(t,),f'SELECT * FROM `{t}` WHERE id BETWEEN %s AND %s ORDER BY id LIMIT %s'),
        Template('insert_'+t,'insert',(t,)),
        Template('update_'+t,'update',(t,))])
for name,tables,sql in [
 ('user_profile',('users','user_profiles'),'SELECT u.id,u.username,p.bio FROM users u LEFT JOIN user_profiles p ON p.user_id=u.id WHERE u.id >= %s ORDER BY u.id LIMIT %s'),
 ('user_address',('users','addresses'),'SELECT u.id,u.username,a.city,a.street FROM users u INNER JOIN addresses a ON a.user_id=u.id WHERE u.id >= %s ORDER BY u.id LIMIT %s'),
 ('product_category',('products','categories'),'SELECT p.id,p.name,p.description,c.name category FROM products p INNER JOIN categories c ON c.id=p.category_id WHERE p.id >= %s ORDER BY p.id LIMIT %s'),
 ('product_reviews',('products','reviews'),'SELECT p.id,p.name,r.rating,r.content FROM products p LEFT JOIN reviews r ON r.product_id=p.id WHERE p.id >= %s ORDER BY p.id LIMIT %s'),
 ('order_user',('orders','users'),'SELECT o.id,o.status,u.username FROM orders o INNER JOIN users u ON u.id=o.user_id WHERE o.id >= %s ORDER BY o.id LIMIT %s'),
 ('order_items_products',('orders','order_items','products'),'SELECT o.id,i.quantity,p.name,p.price FROM orders o INNER JOIN order_items i ON i.order_id=o.id INNER JOIN products p ON p.id=i.product_id WHERE o.id >= %s ORDER BY o.id LIMIT %s'),
 ('order_payment_shipping',('orders','payments','shipments','users'),'SELECT o.id,o.status,p.amount,s.tracking_number,u.username FROM orders o INNER JOIN users u ON u.id=o.user_id LEFT JOIN payments p ON p.order_id=o.id LEFT JOIN shipments s ON s.order_id=o.id WHERE o.id >= %s ORDER BY o.id LIMIT %s'),
 ('suppliers_products',('suppliers','product_suppliers','products'),'SELECT s.id,s.name,p.name product FROM suppliers s INNER JOIN product_suppliers ps ON ps.supplier_id=s.id INNER JOIN products p ON p.id=ps.product_id WHERE s.id >= %s ORDER BY s.id LIMIT %s'),
 ('inventory_product',('product_inventory','products'),'SELECT i.id,i.quantity,p.name FROM product_inventory i INNER JOIN products p ON p.id=i.product_id WHERE i.id >= %s ORDER BY i.id LIMIT %s'),
 ('sessions_users',('sessions','users'),'SELECT s.id,s.expires_at,u.username FROM sessions s INNER JOIN users u ON u.id=s.user_id WHERE s.id >= %s ORDER BY s.id LIMIT %s')]:
    TEMPLATES.append(Template('join_'+name,'join',tables,sql,'HIGH' if len(tables)>2 else 'MEDIUM'))
for name,tables,sql in [
 ('order_totals',('orders','order_items','users'),'SELECT o.id,u.username,COUNT(i.id),SUM(i.quantity*i.price) FROM orders o JOIN users u ON u.id=o.user_id LEFT JOIN order_items i ON i.order_id=o.id WHERE o.id BETWEEN %s AND %s AND o.status IN (\'new\',\'paid\') GROUP BY o.id,u.username LIMIT %s'),
 ('product_ratings',('products','reviews','categories'),'SELECT p.id,c.name,AVG(r.rating),COUNT(r.id) FROM products p JOIN categories c ON c.id=p.category_id LEFT JOIN reviews r ON r.product_id=p.id WHERE p.id BETWEEN %s AND %s AND p.status=\'active\' GROUP BY p.id,c.name LIMIT %s'),
 ('user_orders',('users','orders','payments'),'SELECT u.id,COUNT(DISTINCT o.id),SUM(p.amount) FROM users u LEFT JOIN orders o ON o.user_id=u.id LEFT JOIN payments p ON p.order_id=o.id WHERE u.id BETWEEN %s AND %s AND u.status=\'active\' GROUP BY u.id LIMIT %s'),
 ('inventory_totals',('products','product_inventory','categories'),'SELECT p.id,c.name,SUM(i.quantity),SUM(i.reserved_quantity) FROM products p JOIN categories c ON c.id=p.category_id LEFT JOIN product_inventory i ON i.product_id=p.id WHERE p.id BETWEEN %s AND %s AND p.price>0 GROUP BY p.id,c.name LIMIT %s')]:
    TEMPLATES.append(Template('aggregate_'+name,'aggregate',tables,sql,'HIGH'))
TEMPLATES.extend([
 Template('transaction_order','transaction',('orders','order_items','product_inventory','payments','users','addresses','products')),
 Template('transaction_user','transaction',('users','user_profiles','addresses')),
 Template('transaction_review','transaction',('reviews','users','products')),
 Template('transaction_status','transaction',('orders','audit_events','users'))])

UPDATE_FIELDS = {
 'users':('status', 'active'), 'user_profiles':('bio',None), 'addresses':('city','Demo City'),
 'categories':('description',None), 'products':('description',None), 'product_inventory':('quantity',100),
 'suppliers':('description',None), 'product_suppliers':('cost',10), 'orders':('status','paid'),
 'order_items':('quantity',2), 'payments':('status','confirmed'), 'shipments':('status','shipped'),
 'reviews':('content',None), 'sessions':('user_agent','WorkloadAgent/1.0'), 'audit_events':('payload',None)}
