from typing import Generator, Optional
from omegaconf import OmegaConf
import duckdb


def _make_lazy(func) -> Generator:
    """Make function lazy so it can be deepcopied"""

    def wrapper(*args, **kwargs):
        def generator():
            yield func(*args, **kwargs)

        return generator

    return wrapper


@_make_lazy
def create_duckdb_conn(
    database_name: str, password: Optional[str]
) -> Generator[duckdb.DuckDBPyConnection, None, None]:
    """Create a lazy duckdb connection"""
    config = {"password": password} if password else {}
    conn = duckdb.connect(f"{database_name}.duckdb", config=config)
    return conn


def get_credentials(path) -> str:
    """Retrieve credentials earlier in the lifecycle"""
    data = OmegaConf.load("conf/local/credentials.yml")
    return OmegaConf.select(data, path)


from kedro.io.data_catalog import DataCatalog
