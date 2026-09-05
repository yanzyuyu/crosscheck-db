from abc import ABC, abstractmethod
from typing import Any, Optional


class DatabaseAdapter(ABC):

    @abstractmethod
    def connect(self) -> None:
        pass

    @abstractmethod
    def close(self) -> None:
        pass

    @abstractmethod
    def get_bounds(
        self, table: str, key_col: str, where: Optional[str] = None
    ) -> tuple[int, int, int]:
        pass

    @abstractmethod
    def get_bucket_checksums(
        self,
        table: str,
        key_col: str,
        cols: list[str],
        min_id: int,
        max_id: int,
        bucket_size: int,
        where: Optional[str] = None,
    ) -> dict[int, tuple[int, str]]:
        pass

    @abstractmethod
    def get_row_hashes(
        self,
        table: str,
        key_col: str,
        cols: list[str],
        min_key: int,
        max_key: int,
        where: Optional[str] = None,
    ) -> dict[int, str]:
        pass

    @abstractmethod
    def get_rows(
        self,
        table: str,
        key_col: str,
        cols: list[str],
        keys: list[int],
    ) -> dict[int, dict[str, Any]]:
        pass
