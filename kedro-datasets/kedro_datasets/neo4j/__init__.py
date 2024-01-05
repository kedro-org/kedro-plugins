from typing import Any

import lazy_loader as lazy

Neo4jCypherDataset: Any

__getattr__, __dir__, __all__ = lazy.attach(
    __name__, submod_attrs={"neo4j_dataset": ["Neo4jCypherDataset"]}
)
