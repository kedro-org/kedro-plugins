from __future__ import annotations

from typing import Any

import pytest
from kedro.io import AbstractDataset, DataCatalog, MemoryDataset
from kedro.pipeline import node
from kedro.pipeline.modular_pipeline import pipeline as modular_pipeline

from kedro_airflow.grouping import _is_memory_dataset, group_memory_nodes


class TestDataset(AbstractDataset):
    def _save(self, data) -> None:
        pass

    def _describe(self) -> dict[str, Any]:
        return {}

    def _load(self):
        return []


@pytest.mark.parametrize(
    "memory_nodes,expected_nodes,expected_dependencies",
    [
        (
            ["ds3", "ds6"],
            [["f1"], ["f2", "f3", "f4", "f6", "f7"], ["f5"]],
            {"f1": ["f2_f3_f4_f6_f7"], "f2_f3_f4_f6_f7": ["f5"]},
        ),
        (
            ["ds3"],
            [["f1"], ["f2", "f3", "f4", "f7"], ["f5"], ["f6"]],
            {"f1": ["f2_f3_f4_f7"], "f2_f3_f4_f7": ["f5", "f6"]},
        ),
        (
            [],
            [["f1"], ["f2"], ["f3"], ["f4"], ["f5"], ["f6"], ["f7"]],
            {"f1": ["f2"], "f2": ["f3", "f4", "f5", "f7"], "f4": ["f6", "f7"]},
        ),
    ],
)
def test_group_memory_nodes(
    memory_nodes: list[str],
    expected_nodes: list[list[str]],
    expected_dependencies: dict[str, list[str]],
):
    """Check the grouping of memory nodes."""
    nodes = [f"ds{i}" for i in range(1, 10)]
    assert all(node_name in nodes for node_name in memory_nodes)

    mock_catalog = DataCatalog()
    for dataset_name in nodes:
        if dataset_name in memory_nodes:
            dataset = MemoryDataset()
        else:
            dataset = TestDataset()
        mock_catalog.add(dataset_name, dataset)

    def identity_one_to_one(x):
        return x

    mock_pipeline = modular_pipeline(
        [
            node(
                func=identity_one_to_one,
                inputs="ds1",
                outputs="ds2",
                name="f1",
            ),
            node(
                func=lambda x: (x, x),
                inputs="ds2",
                outputs=["ds3", "ds4"],
                name="f2",
            ),
            node(
                func=identity_one_to_one,
                inputs="ds3",
                outputs="ds5",
                name="f3",
            ),
            node(
                func=identity_one_to_one,
                inputs="ds3",
                outputs="ds6",
                name="f4",
            ),
            node(
                func=identity_one_to_one,
                inputs="ds4",
                outputs="ds8",
                name="f5",
            ),
            node(
                func=identity_one_to_one,
                inputs="ds6",
                outputs="ds7",
                name="f6",
            ),
            node(
                func=lambda x, y: x,
                inputs=["ds3", "ds6"],
                outputs="ds9",
                name="f7",
            ),
        ],
    )

    nodes, dependencies = group_memory_nodes(mock_catalog, mock_pipeline)
    sequence = [
        [node_.name for node_ in node_sequence] for node_sequence in nodes.values()
    ]
    assert sequence == expected_nodes
    assert dict(dependencies) == expected_dependencies


def test_is_memory_dataset():
    catalog = DataCatalog()
    catalog.add("parameters", {"hello": "world"})
    catalog.add("params:hello", "world")
    catalog.add("my_dataset", MemoryDataset(True))
    catalog.add("test_dataset", TestDataset())
    assert not _is_memory_dataset(catalog, "parameters")
    assert not _is_memory_dataset(catalog, "params:hello")
    assert _is_memory_dataset(catalog, "my_dataset")
    assert not _is_memory_dataset(catalog, "test_dataset")
