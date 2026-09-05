from dataclasses import dataclass, field
from typing import Optional


@dataclass
class DatabaseConfig:
    driver: str
    database: str
    host: Optional[str] = None
    port: Optional[int] = None
    user: Optional[str] = None
    password: Optional[str] = None
    project_id: Optional[str] = None
    dataset_id: Optional[str] = None


@dataclass
class TableConfig:
    name: str
    key_column: str
    columns: list[str]
    bucket_size: int = 5000
    where_clause: Optional[str] = None
