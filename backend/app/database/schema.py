from dataclasses import dataclass
from app.models.config import quote

@dataclass(frozen=True)
class Table:
    name: str
    columns: str
    insert_columns: tuple[str, ...]
    refs: tuple[tuple[str,str], ...] = ()
    indexes: tuple[str, ...] = ()

TABLES = [
 Table('users', 'username VARCHAR(100) NOT NULL, email VARCHAR(180) NOT NULL, first_name VARCHAR(60), last_name VARCHAR(60), status VARCHAR(20), created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP', ('username','email','first_name','last_name','status'), indexes=('email','status')),
 Table('user_profiles','user_id BIGINT UNSIGNED NOT NULL, phone VARCHAR(40), birth_date DATE, bio LONGTEXT, preferences JSON, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP',('user_id','phone','birth_date','bio','preferences'),(('user_id','users'),)),
 Table('addresses','user_id BIGINT UNSIGNED NOT NULL, country VARCHAR(60), city VARCHAR(80), street VARCHAR(160), postal_code VARCHAR(20), address_type VARCHAR(20), created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP',('user_id','country','city','street','postal_code','address_type'),(('user_id','users'),)),
 Table('categories','parent_id BIGINT UNSIGNED NULL, name VARCHAR(100), description LONGTEXT, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP',('parent_id','name','description'),(('parent_id','categories'),)),
 Table('products','category_id BIGINT UNSIGNED NOT NULL, sku VARCHAR(100), name VARCHAR(160), description LONGTEXT, price DECIMAL(12,2), status VARCHAR(20), created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP',('category_id','sku','name','description','price','status'),(('category_id','categories'),),('sku','status')),
 Table('product_inventory','product_id BIGINT UNSIGNED NOT NULL, warehouse_code VARCHAR(40), quantity INT NOT NULL, reserved_quantity INT NOT NULL, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP',('product_id','warehouse_code','quantity','reserved_quantity'),(('product_id','products'),)),
 Table('suppliers','name VARCHAR(100), email VARCHAR(180), description LONGTEXT',('name','email','description')),
 Table('product_suppliers','product_id BIGINT UNSIGNED NOT NULL, supplier_id BIGINT UNSIGNED NOT NULL, cost DECIMAL(12,2)',('product_id','supplier_id','cost'),(('product_id','products'),('supplier_id','suppliers'))),
 Table('orders','user_id BIGINT UNSIGNED NOT NULL, address_id BIGINT UNSIGNED NOT NULL, status VARCHAR(20), total_amount DECIMAL(12,2), created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP',('user_id','address_id','status','total_amount'),(('user_id','users'),('address_id','addresses')),('status','created_at')),
 Table('order_items','order_id BIGINT UNSIGNED NOT NULL, product_id BIGINT UNSIGNED NOT NULL, quantity INT NOT NULL, price DECIMAL(12,2), created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP',('order_id','product_id','quantity','price'),(('order_id','orders'),('product_id','products'))),
 Table('payments','order_id BIGINT UNSIGNED NOT NULL, payment_method VARCHAR(30), amount DECIMAL(12,2), status VARCHAR(20), transaction_reference VARCHAR(100), created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP',('order_id','payment_method','amount','status','transaction_reference'),(('order_id','orders'),),('status',)),
 Table('shipments','order_id BIGINT UNSIGNED NOT NULL, tracking_number VARCHAR(100), carrier VARCHAR(40), status VARCHAR(20), shipped_at DATETIME, delivered_at DATETIME',('order_id','tracking_number','carrier','status','shipped_at','delivered_at'),(('order_id','orders'),),('tracking_number',)),
 Table('reviews','user_id BIGINT UNSIGNED NOT NULL, product_id BIGINT UNSIGNED NOT NULL, rating INT, title VARCHAR(100), content LONGTEXT, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP',('user_id','product_id','rating','title','content'),(('user_id','users'),('product_id','products')),('rating',)),
 Table('sessions','user_id BIGINT UNSIGNED NOT NULL, session_token_hash CHAR(64), ip_address VARCHAR(45), user_agent VARCHAR(120), created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, expires_at DATETIME',('user_id','session_token_hash','ip_address','user_agent','expires_at'),(('user_id','users'),)),
 Table('audit_events','user_id BIGINT UNSIGNED NOT NULL, order_id BIGINT UNSIGNED NULL, event_type VARCHAR(40), payload LONGTEXT, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP',('user_id','order_id','event_type','payload'),(('user_id','users'),('order_id','orders')),('created_at',))
]
BY_NAME = {t.name:t for t in TABLES}
NAMES = tuple(BY_NAME)

def ddl(t: Table, foreign_keys: bool) -> str:
    constraints = ', '.join(f'FOREIGN KEY ({quote(col)}) REFERENCES {quote(parent)} (id)' for col,parent in t.refs)
    return f'CREATE TABLE {quote(t.name)} (id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY, {t.columns}' + (', '+constraints if foreign_keys and constraints else '') + ') ENGINE=InnoDB'

def insert_sql(t: Table) -> str:
    return f'INSERT INTO {quote(t.name)} ({",".join(quote(c) for c in t.insert_columns)}) VALUES ({",".join("%s" for _ in t.insert_columns)})'
