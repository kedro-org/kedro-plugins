"""`FAISSVectorStoreDataset` connects to a FAISS index as a vector store."""

from __future__ import annotations

import json
import os
import uuid
from collections import Counter
from collections.abc import Callable
from pathlib import Path
from typing import Any, Literal

import faiss
import numpy as np
from kedro.io.core import DatasetError

from kedro_datasets_experimental.vectorstore_base import (
    AbstractVectorStoreDataset,
    VectorStoreHandle,
)

_METRIC_MAP = {"l2": faiss.METRIC_L2, "ip": faiss.METRIC_INNER_PRODUCT}
_VECTOR_ARRAY_NDIM = 2


class FAISSVectorStoreHandle(VectorStoreHandle):
    """Handle for interacting with a FAISS index.

    Returned by `FAISSVectorStoreDataset.load()`. Unlike the Weaviate/Chroma
    handles, this one owns no live connection — FAISS is a local, in-process
    library, not a client/server. It also owns machinery those backends get
    for free from their client libraries: FAISS itself only stores vectors and
    `int64` IDs, so this handle maintains its own string-ID mapping and a
    `properties` side-store, and is responsible for its own persistence.

    Persistence is deliberately explicit, not automatic: `add()`/`delete()`
    never write to disk, and `close()` is a no-op that does **not** persist
    anything. Call `save()` yourself when you want the current state written
    out::

        with catalog.load("my_index") as store:
            store.add([{"properties": {"text": "hello"}, "vector": [0.1, 0.2]}])
            store.save()

    `train()` is required before `add()` for `index_factory` choices that need
    training (e.g. `"IVF100,Flat"`); the default `"Flat"` never needs it.

    `raw_client` exposes the underlying FAISS `IndexIDMap`-wrapped index for
    anything not covered by this interface.
    """

    def __init__(
        self,
        index: Any,
        index_path: str | None,
        next_id: int = 0,
        id_to_internal: dict[str, int] | None = None,
        properties: dict[str, dict[str, Any]] | None = None,
    ) -> None:
        self._index = index
        self._index_path = index_path
        self._next_id = next_id
        self._id_to_internal: dict[str, int] = dict(id_to_internal or {})
        self._internal_to_id: dict[int, str] = {
            internal_id: str_id for str_id, internal_id in self._id_to_internal.items()
        }
        self._properties: dict[str, dict[str, Any]] = dict(properties or {})

    @property
    def raw_client(self) -> Any:
        """The underlying FAISS `IndexIDMap`-wrapped index."""
        return self._index

    def close(self) -> None:
        """No-op. FAISS is a local object, not a live connection — there is
        nothing to release. This does **not** persist anything; call `save()`
        explicitly if you want your data to survive past this handle's
        lifetime.
        """

    def describe(self) -> dict[str, Any]:
        """Return the configured path (or `"<in-memory>"`) and current record count."""
        return {
            "collection": self._index_path or "<in-memory>",
            "count": self._index.ntotal,
        }

    def train(self, vectors: list[list[float]]) -> None:
        """Train the underlying FAISS index.

        Required before `add()` for `index_factory` choices that need training
        (e.g. `"IVF100,Flat"`). Not needed for `"Flat"` (the default), which is
        always trained. Not called automatically by `add()` — training quality
        depends on the training set being representative, and picking that set
        on the caller's behalf is not this handle's decision to make.

        Args:
            vectors: Training vectors, used to fit the index's internal
                quantizer/clustering. Should be representative of the data
                you'll actually add.

        Raises:
            DatasetError: If the underlying FAISS `train()` call fails.
        """
        vectors_np = np.asarray(vectors, dtype="float32")
        try:
            self._index.train(vectors_np)
        except Exception as e:
            raise DatasetError(f"train() failed: {e}") from e

    def add(self, records: list[dict[str, Any]]) -> list[str]:
        """Insert records into the index and return their IDs.

        Every record must carry a `"vector"` — FAISS has no embedding function
        to fall back on. Records whose explicit `"id"` already exists in the
        index, or that collide with another record's `"id"` in the same batch,
        are rejected all-or-nothing: nothing is written if any collision is
        found.

        Args:
            records: Each record is a dict with keys `"properties"` (dict,
                optional), `"vector"` (list[float], required), `"id"` (str,
                optional — generated if omitted).

        Returns:
            List of ID strings for the inserted records, in input order.

        Raises:
            DatasetError: If the index isn't trained yet, if any record is
                missing `"vector"`, if any ID collides, or if the underlying
                FAISS call fails.
        """
        if not records:
            return []

        if not self._index.is_trained:
            raise DatasetError(
                "add() failed: the FAISS index is not trained. This "
                "index_factory choice requires training — call "
                "handle.train(vectors) before adding records."
            )

        explicit_ids = [record["id"] for record in records if record.get("id") is not None]
        counts = Counter(explicit_ids)
        collisions = {id_ for id_, count in counts.items() if count > 1} | {
            id_ for id_ in explicit_ids if id_ in self._id_to_internal
        }
        if collisions:
            raise DatasetError(
                f"add() rejected {len(collisions)} record(s) with colliding "
                f"IDs: {sorted(collisions)}. IDs must be unique within the "
                "batch and not already exist in the index."
            )

        for record in records:
            if record.get("vector") is None:
                raise DatasetError(
                    "add() failed: every record requires a 'vector' — FAISS "
                    "has no embedding function to generate one."
                )

        str_ids = [record.get("id") or str(uuid.uuid4()) for record in records]
        vectors = [record["vector"] for record in records]
        props_list = [record.get("properties", {}) for record in records]
        internal_ids = list(range(self._next_id, self._next_id + len(records)))

        vectors_np = np.asarray(vectors, dtype="float32")
        if vectors_np.ndim != _VECTOR_ARRAY_NDIM or vectors_np.shape[1] != self._index.d:
            raise DatasetError(
                f"add() failed: vector dimension does not match the index "
                f"dimension {self._index.d}."
            )
        ids_np = np.asarray(internal_ids, dtype="int64")

        try:
            self._index.add_with_ids(vectors_np, ids_np)
        except Exception as e:
            raise DatasetError(f"add() failed: {e}") from e

        self._next_id += len(records)
        for str_id, internal_id, props in zip(str_ids, internal_ids, props_list):
            self._id_to_internal[str_id] = internal_id
            self._internal_to_id[internal_id] = str_id
            self._properties[str_id] = props

        return str_ids

    def delete(
        self,
        *,
        ids: list[str] | None = None,
        filters: Callable[[dict[str, Any]], bool] | None = None,
    ) -> None:
        """Delete records from the index by ID or metadata predicate.

        Exactly one of `ids` or `filters` must be provided.

        Args:
            ids: List of ID strings to delete. IDs that aren't present are
                silently skipped (no error).
            filters: A callable `(properties: dict) -> bool` evaluated against
                every stored record's properties; matching records are
                deleted. This is an **O(n) linear scan** over the in-memory
                properties store — FAISS has no native metadata index to push
                this down to.

        Raises:
            DatasetError: If neither or both arguments are supplied, or if the
                underlying FAISS call fails.
        """
        if ids is None and filters is None:
            raise DatasetError("delete() requires exactly one of 'ids' or 'filters'.")
        if ids is not None and filters is not None:
            raise DatasetError("delete() accepts 'ids' or 'filters', not both.")

        if filters is not None:
            target_ids = [
                str_id for str_id, props in self._properties.items() if filters(props)
            ]
        else:
            target_ids = ids

        internal_ids = [
            self._id_to_internal[str_id]
            for str_id in target_ids
            if str_id in self._id_to_internal
        ]
        if not internal_ids:
            return

        try:
            selector = faiss.IDSelectorBatch(np.asarray(internal_ids, dtype="int64"))
            self._index.remove_ids(selector)
        except Exception as e:
            raise DatasetError(f"delete() failed: {e}") from e

        for internal_id in internal_ids:
            str_id = self._internal_to_id.pop(internal_id, None)
            if str_id is not None:
                self._id_to_internal.pop(str_id, None)
                self._properties.pop(str_id, None)

    def search(
        self,
        *,
        vector: list[float] | None = None,
        text: str | None = None,
        top_k: int = 10,
        filters: Callable[[dict[str, Any]], bool] | None = None,
    ) -> list[dict[str, Any]]:
        """Search the index by vector and return the top matches.

        `text=...` is never supported — FAISS has no embedding function, ever
        (unlike Chroma, which has an optional default one).

        Args:
            vector: Query embedding for similarity search. Required (the only
                supported search mode).
            text: Not supported; always raises `NotImplementedError` if given.
            top_k: Maximum number of results to return. Defaults to 10.
            filters: A callable `(properties: dict) -> bool` restricting the
                search to matching records. Evaluated as an O(n) linear scan
                over the properties store, then pushed into FAISS's own search
                via an `IDSelector` (not applied as post-filtering on the
                results, which would under-count matches).

        Returns:
            List of result dicts, each containing `"id"` (str), `"properties"`
            (dict), and `"distance"` (float).

        Raises:
            NotImplementedError: If `text` is given.
            DatasetError: If neither or both of `vector`/`text` are supplied,
                or if the underlying FAISS call fails.
        """
        if vector is None and text is None:
            raise DatasetError("search() requires exactly one of 'vector' or 'text'.")
        if vector is not None and text is not None:
            raise DatasetError("search() accepts 'vector' or 'text', not both.")
        if text is not None:
            raise NotImplementedError(
                "search(text=...) is not supported by FAISSVectorStoreHandle — "
                "FAISS has no embedding function. Pass vector= with a "
                "precomputed embedding instead."
            )

        params = None
        if filters is not None:
            candidate_ids = [
                self._id_to_internal[str_id]
                for str_id, props in self._properties.items()
                if filters(props)
            ]
            selector = faiss.IDSelectorBatch(np.asarray(candidate_ids, dtype="int64"))
            params = faiss.SearchParameters(sel=selector)

        query = np.asarray([vector], dtype="float32")
        try:
            distances, internal_ids = self._index.search(query, k=top_k, params=params)
        except Exception as e:
            raise DatasetError(f"search() failed: {e}") from e

        results = []
        for internal_id, distance in zip(internal_ids[0], distances[0]):
            if internal_id == -1:
                continue
            str_id = self._internal_to_id.get(int(internal_id))
            if str_id is None:
                continue
            results.append(
                {
                    "id": str_id,
                    "properties": self._properties.get(str_id, {}),
                    "distance": float(distance),
                }
            )
        return results

    def save(self, path: str | None = None) -> None:
        """Write the index and its metadata sidecar to disk.

        The **only** persistence trigger — never called implicitly by
        `add()`, `delete()`, or `close()`. Behaves like a thin wrapper around
        what you'd do by hand with `raw_client` + `faiss.write_index()`: no
        remembered path (`path=` here doesn't change what a later bare
        `save()` defaults to), silent overwrite, local filesystem only.

        The one addition beyond a literal passthrough: the index file is
        written via a temp-file-then-`os.replace()` swap, so a crash mid-write
        can't corrupt a previously-good file. The index file and the metadata
        sidecar (`f"{path}.meta.json"`) are still two separate writes — a
        crash between them leaves the two out of sync; this is not solved,
        just avoided for the index file itself.

        Args:
            path: Where to write the index (and `f"{path}.meta.json"` for the
                sidecar). Defaults to the `index_path` passed to the dataset's
                constructor.

        Raises:
            DatasetError: If neither `path` nor a constructor `index_path` is
                set, or if either write fails.
        """
        target = path or self._index_path
        if target is None:
            raise DatasetError(
                "save() requires a 'path' argument, since no 'index_path' "
                "was configured on the dataset."
            )

        tmp_path = f"{target}.tmp"
        try:
            faiss.write_index(self._index, tmp_path)
            os.replace(tmp_path, target)
        except Exception as e:
            raise DatasetError(f"save() failed while writing the FAISS index: {e}") from e

        meta = {
            "next_id": self._next_id,
            "id_to_internal": self._id_to_internal,
            "properties": self._properties,
        }
        try:
            Path(f"{target}.meta.json").write_text(json.dumps(meta))
        except Exception as e:
            raise DatasetError(
                f"save() failed while writing the metadata sidecar: {e}"
            ) from e


class FAISSVectorStoreDataset(AbstractVectorStoreDataset):
    """Connect to a FAISS index and return a `FAISSVectorStoreHandle`.

    Unlike `weaviate.WeaviateVectorStoreDataset`/`chromadb.ChromaDBDataset`,
    FAISS is a pure numerical index library — no server, no client, no
    metadata storage, no persistence beyond raw index files, no embedding
    function. This dataset and its handle build the ID-mapping, metadata
    side-store, and persistence machinery themselves, and deliberately expose
    more of the library directly than the other two backends do: users who
    reach for FAISS specifically want a less-abstracted, hands-on experience.

    `load()` creates or hydrates a FAISS index (wrapped in an `IndexIDMap` for
    string-ID support) and returns a handle. All read/write operations go
    through the handle. `save()` is intentionally disabled on the dataset and
    raises `DatasetError` — persistence goes through `handle.save()`, called
    explicitly, never automatically.

    `index_path` is optional. If given and both the index file and its
    `f"{index_path}.meta.json"` sidecar exist, `load()` hydrates from disk. If
    given but absent, `load()` starts a fresh index (nothing is written until
    `handle.save()` is called). If omitted entirely, the handle is pure
    in-memory — data does not survive past the handle's lifetime unless
    `handle.save(path=...)` is called with an explicit path.

    `index_factory` is passed straight to FAISS's own `index_factory()` —
    `"Flat"` (the default) is exact search and needs no training; approximate
    types like `"IVF100,Flat"` or `"HNSW32"` require calling `handle.train()`
    before `add()`.

    Examples:
        Using the [YAML API](https://docs.kedro.org/en/stable/catalog-data/data_catalog_yaml_examples/):

        ```yaml
        my_index:
          type: faiss.FAISSVectorStoreDataset
          dimension: 384
          index_path: data/06_models/my_index.faiss
        ```

        Using the [Python API](https://docs.kedro.org/en/stable/catalog-data/advanced_data_catalog_usage/):

        >>> from kedro_datasets_experimental.faiss import FAISSVectorStoreDataset
        >>> dataset = FAISSVectorStoreDataset(dimension=3)
        >>> with dataset.load() as store:
        ...     ids = store.add(
        ...         [{"properties": {"text": "hello"}, "vector": [0.1, 0.2, 0.3]}]
        ...     )
        ...     hits = store.search(vector=[0.1, 0.2, 0.3], top_k=5)
        ...     store.save(path="/tmp/my_index.faiss")
    """

    def __init__(
        self,
        *,
        dimension: int,
        index_path: str | None = None,
        index_factory: str = "Flat",
        metric: Literal["l2", "ip"] = "l2",
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """Create a new `FAISSVectorStoreDataset`.

        Args:
            dimension: Dimensionality of the vectors to store. Always
                required — FAISS cannot create an index without it, and there
                is no reliable way to infer it before the first `add()` call.
            index_path: Path to persist to/hydrate from. See the class
                docstring for the three cases (hydrate / fresh-at-path /
                pure in-memory). Local filesystem only — no `fsspec` support.
            index_factory: FAISS index factory string, e.g. `"Flat"` (default,
                exact search, no training needed) or `"IVF100,Flat"`/
                `"HNSW32"` (approximate, requires `handle.train()` first).
            metric: `"l2"` (default) or `"ip"` (inner product). No built-in
                cosine similarity — normalize vectors yourself and use `"ip"`
                if that's what you want, the standard FAISS convention.
            metadata: Arbitrary metadata passed through by Kedro; ignored by
                this dataset.
        """
        if metric not in _METRIC_MAP:
            raise DatasetError(f"Unknown metric: '{metric}'. Must be one of: 'l2', 'ip'.")
        self._dimension = dimension
        self._index_path = index_path
        self._index_factory = index_factory
        self._metric = metric
        self.metadata = metadata

    def _load(self) -> FAISSVectorStoreHandle:
        index_file = Path(self._index_path) if self._index_path else None
        meta_file = Path(f"{self._index_path}.meta.json") if self._index_path else None

        if (
            index_file is not None
            and meta_file is not None
            and index_file.exists()
            and meta_file.exists()
        ):
            try:
                index = faiss.read_index(str(index_file))
            except Exception as e:
                raise DatasetError(f"Failed to read FAISS index from '{index_file}': {e}") from e
            if index.d != self._dimension:
                raise DatasetError(
                    f"Loaded FAISS index at '{index_file}' has dimension "
                    f"{index.d}, which does not match the configured "
                    f"dimension {self._dimension}."
                )
            try:
                meta = json.loads(meta_file.read_text())
            except Exception as e:
                raise DatasetError(
                    f"Failed to read FAISS metadata sidecar from '{meta_file}': {e}"
                ) from e
            next_id = meta["next_id"]
            id_to_internal = meta["id_to_internal"]
            properties = meta["properties"]
        else:
            try:
                base = faiss.index_factory(
                    self._dimension, self._index_factory, _METRIC_MAP[self._metric]
                )
            except Exception as e:
                raise DatasetError(
                    f"Failed to create FAISS index (index_factory="
                    f"'{self._index_factory}'): {e}"
                ) from e
            index = faiss.IndexIDMap(base)
            next_id = 0
            id_to_internal = {}
            properties = {}

        return FAISSVectorStoreHandle(
            index=index,
            index_path=self._index_path,
            next_id=next_id,
            id_to_internal=id_to_internal,
            properties=properties,
        )

    def _describe(self) -> dict[str, Any]:
        return {
            "dimension": self._dimension,
            "index_path": self._index_path,
            "index_factory": self._index_factory,
            "metric": self._metric,
        }
