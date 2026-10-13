import os
import re
from typing import Literal
from pydantic import BaseModel, Field, SecretStr, model_validator, field_validator, ConfigDict

LIMITS = {k: int(os.getenv(k, v)) for k, v in {
    'MAX_WORKERS': '500', 'MAX_RPS': '100000', 'MAX_PAYLOAD_BYTES': '5242880',
    'MAX_CONNECTIONS': '1000', 'MAX_GENERATOR_ROWS': '20000000'}.items()}

if LIMITS['MAX_CONNECTIONS'] < 3 or LIMITS['MAX_WORKERS'] < 1 or LIMITS['MAX_RPS'] < 1 or LIMITS['MAX_PAYLOAD_BYTES'] < 0 or LIMITS['MAX_GENERATOR_ROWS'] < 150:
    raise ValueError('Invalid server limits: connections >= 3, workers/RPS >= 1, payload >= 0, generator rows >= 150')

def identifier(value: str) -> str:
    if not re.fullmatch(r'[A-Za-z][A-Za-z0-9_]{0,63}', value):
        raise ValueError('Use an SQL identifier: letter followed by up to 63 letters, digits or underscores')
    return value

def quote(value: str) -> str:
    return '`' + identifier(value) + '`'

class ConnectionConfig(BaseModel):
    host: str = Field(default='host.docker.internal', min_length=1, max_length=253)
    port: int = Field(default=3306, ge=1, le=65535)
    username: str = Field(default='workload', min_length=1, max_length=128)
    password: SecretStr | None = Field(default=None, exclude=True)
    database: str = 'agent_test'
    table: str = 'workload_agent_data'
    connection_timeout: float = Field(default=5, ge=0.2, le=120)
    retries: int = Field(default=3, ge=0, le=20)
    @field_validator('database', 'table')
    @classmethod
    def valid_identifier(cls, v: str) -> str:
        return identifier(v)

class WorkloadConfig(BaseModel):
    model_config = ConfigDict(validate_default=True)
    mode: Literal['NORMAL', 'INTEGRITY', 'MIXED'] = 'NORMAL'
    integrity_percent: float = Field(default=20, ge=0, le=100)
    integrity_test: bool = False
    duration_seconds: int = Field(default=0, ge=0, le=2592000)
    max_requests: int = Field(default=0, ge=0, le=1000000000)
    workers: int = Field(default=min(10,LIMITS['MAX_WORKERS']), ge=1, le=LIMITS['MAX_WORKERS'])
    rps: float = Field(default=min(100,LIMITS['MAX_RPS']), gt=0, le=LIMITS['MAX_RPS'])
    min_payload: int = Field(default=min(100,LIMITS['MAX_PAYLOAD_BYTES']), ge=0, le=LIMITS['MAX_PAYLOAD_BYTES'])
    max_payload: int = Field(default=min(50000,LIMITS['MAX_PAYLOAD_BYTES']), ge=0, le=LIMITS['MAX_PAYLOAD_BYTES'])
    select_percent: float = Field(default=70, ge=0, le=100)
    insert_percent: float = Field(default=20, ge=0, le=100)
    update_percent: float = Field(default=10, ge=0, le=100)
    simple_percent: float = Field(default=25, ge=0, le=100)
    join_percent: float = Field(default=20, ge=0, le=100)
    aggregate_percent: float = Field(default=10, ge=0, le=100)
    relational_insert_percent: float = Field(default=15, ge=0, le=100)
    relational_update_percent: float = Field(default=15, ge=0, le=100)
    transaction_percent: float = Field(default=15, ge=0, le=100)
    min_rows: int = Field(default=1, ge=1, le=1000)
    max_rows: int = Field(default=100, ge=1, le=1000)
    delay_ms: int = Field(default=0, ge=0, le=60000)
    rate_mode: Literal['PACED', 'TOKEN_BUCKET'] = 'PACED'
    pool_min_size: int = Field(default=1, ge=1, le=LIMITS['MAX_CONNECTIONS'])
    pool_max_size: int = Field(default=min(10,LIMITS['MAX_CONNECTIONS']-2), ge=1, le=LIMITS['MAX_CONNECTIONS'] - 2)
    connection_refresh_every: int = Field(default=5, ge=1, le=1000000)
    query_timeout: float = Field(default=10, gt=0, le=300)
    reconnect_timeout: float = Field(default=10, ge=1, le=120)
    slow_threshold_ms: float = Field(default=500, gt=0)
    auto_create_table: bool = True
    relational: bool = False
    complexity: Literal['LOW', 'MEDIUM', 'HIGH', 'RANDOM'] = 'RANDOM'
    payload_distribution: Literal['UNIFORM', 'RANDOM', 'SMALL_HEAVY', 'LARGE_HEAVY'] = 'UNIFORM'
    index_mode: Literal['INDEXED', 'MIXED'] = 'INDEXED'
    @model_validator(mode='before')
    @classmethod
    def migrate_mode(cls, values):
        if isinstance(values, dict):
            values = dict(values)
            if 'mode' not in values:
                values['mode'] = 'INTEGRITY' if values.get('integrity_test', False) else 'NORMAL'
        return values

    @model_validator(mode='after')
    def check(self):
        self.integrity_test = self.mode == 'INTEGRITY'
        if self.mode == 'MIXED' and not 0 < self.integrity_percent < 100:
            raise ValueError('Mixed mode integrity percentage must be greater than 0 and less than 100')
        if self.min_payload > self.max_payload or self.min_rows > self.max_rows:
            raise ValueError('Minimum must not exceed maximum')
        if self.pool_min_size > self.pool_max_size:
            raise ValueError('Pool minimum must not exceed maximum')
        if abs(self.select_percent + self.insert_percent + self.update_percent - 100) > .001:
            raise ValueError('SELECT + INSERT + UPDATE must equal 100')
        if abs(sum(getattr(self, k) for k in ('simple_percent','join_percent','aggregate_percent',
                  'relational_insert_percent','relational_update_percent','transaction_percent')) - 100) > .001:
            raise ValueError('Relational workload percentages must equal 100')
        return self

class Config(BaseModel):
    connection: ConnectionConfig = Field(default_factory=ConnectionConfig)
    workload: WorkloadConfig = Field(default_factory=WorkloadConfig)

class DatabaseRequest(BaseModel):
    model_config = ConfigDict(validate_default=True)
    name: str
    preset: Literal['SMALL','MEDIUM','LARGE','VERY LARGE','CUSTOM'] = 'SMALL'
    records: int = Field(default=min(10000,LIMITS['MAX_GENERATOR_ROWS']), ge=150, le=LIMITS['MAX_GENERATOR_ROWS'])
    payload_size: int = Field(default=min(256,LIMITS['MAX_PAYLOAD_BYTES']), ge=0, le=LIMITS['MAX_PAYLOAD_BYTES'])
    batch_size: int = Field(default=500, ge=1, le=5000)
    create_indexes: bool = True
    create_foreign_keys: bool = True
    generate_initial_data: bool = True
    confirmation: str
    @field_validator('name')
    @classmethod
    def name_check(cls, v):
        identifier(v)
        if not v.startswith('workload_test_'):
            raise ValueError('Managed databases must start with workload_test_')
        return v
    @model_validator(mode='after')
    def confirm(self):
        if self.confirmation != self.name:
            raise ValueError('Type the exact database name to confirm')
        return self
