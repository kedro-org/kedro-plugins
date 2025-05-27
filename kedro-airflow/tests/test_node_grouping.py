from __future__ import annotations

from typing import Any

import pytest
from kedro.io import AbstractDataset, DataCatalog, MemoryDataset
from kedro.pipeline import Pipeline, node
from kedro.pipeline.modular_pipeline import pipeline as modular_pipeline

from kedro_airflow.grouping import (
    _is_memory_dataset,
    group_memory_nodes,
)


class TestDataset(AbstractDataset):
    def _save(self, data) -> None:
        pass

    def _describe(self) -> dict[str, Any]:
        return {}

    def _load(self):
        return []


def mock_data_catalog(
    nodes: list[str], memory_nodes: set[str], memory_nodes_in_catalog: bool = False
) -> DataCatalog:
    mock_catalog = DataCatalog()
    for dataset_name in nodes:
        if dataset_name not in memory_nodes:
            dataset = TestDataset()
            mock_catalog.add(dataset_name, dataset)
        elif memory_nodes_in_catalog:
            mock_catalog.add(dataset_name, MemoryDataset())

    return mock_catalog


def mock_kedro_pipeline() -> Pipeline:
    def identity_one_to_one(x):
        return x

    return modular_pipeline(
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


@pytest.fixture
def mock_pipeline_not_namespaced():
    base_pipeline = mock_kedro_pipeline()
    return modular_pipeline(base_pipeline)


@pytest.fixture
def mock_pipeline_namespaced():
    base_pipeline = mock_kedro_pipeline()
    return modular_pipeline(base_pipeline, namespace="namespace1")


@pytest.mark.parametrize(
    "all_nodes, memory_nodes, expected_nodes, expected_dependencies",
    [
        (
            ["ds1", "ds2", "ds3", "ds4", "ds5", "ds6", "ds7", "ds8", "ds9"],
            {"ds3", "ds6"},
            [["f1"], ["f2", "f3", "f4", "f6", "f7"], ["f5"]],
            {
                "f2_f3_f4_f6_f7": {"f1"},
                "f5": {"f2_f3_f4_f6_f7"},
            },
        ),
        (
            ["ds1", "ds2", "ds3", "ds4", "ds5", "ds6", "ds7", "ds8", "ds9"],
            {"ds3"},
            [["f1"], ["f2", "f3", "f4", "f7"], ["f5"], ["f6"]],
            {
                "f2_f3_f4_f7": {"f1"},
                "f5": {"f2_f3_f4_f7"},
                "f6": {"f2_f3_f4_f7"},
            },
        ),
        (
            ["ds1", "ds2", "ds3", "ds4", "ds5", "ds6", "ds7", "ds8", "ds9"],
            set(),
            [["f1"], ["f2"], ["f3"], ["f4"], ["f5"], ["f6"], ["f7"]],
            {
                "f2": {"f1"},
                "f3": {"f2"},
                "f4": {"f2"},
                "f5": {"f2"},
                "f6": {"f4"},
                "f7": {"f2", "f4"},
            },
        ),
    ],
)
def test_group_memory_nodes(
    all_nodes: list[str],
    memory_nodes: set[str],
    expected_nodes: list[list[str]],
    expected_dependencies: dict[str, set[str]],
):
    """Check the grouping of memory nodes."""
    mock_catalog = mock_data_catalog(all_nodes, memory_nodes)
    mock_pipeline = mock_kedro_pipeline()

    result = group_memory_nodes(mock_catalog, mock_pipeline)

    # Extract node name groups (sorted inside and overall, to ensure deterministic comparison)
    actual_nodes = [
        sorted([node.name for node in group_info["nodes"]])
        for group_info in result.values()
    ]
    assert sorted(actual_nodes) == sorted(expected_nodes)

    # Extract dependencies
    actual_dependencies = {
        group_name: set(group_info["dependencies"])
        for group_name, group_info in result.items()
        if group_info["dependencies"]
    }
    assert actual_dependencies == expected_dependencies


@pytest.mark.parametrize(
    "nodes,memory_nodes",
    [
        (
            ["ds0", "ds1", "ds2"],
            {"ds0", "ds1", "ds2"},
        ),
        (
            ["ds0", "ds1", "ds2"],
            {"ds0"},
        ),
        (
            ["ds0", "ds1", "ds2"],
            {},
        ),
    ],
)
@pytest.mark.parametrize("memory_nodes_in_catalog", (True, False))
def test_is_memory_dataset(
    nodes: list[str], memory_nodes: set[str], memory_nodes_in_catalog: bool
):
    """Tests for the `_is_memory_dataset` function.

    Args:
        nodes: list of nodes to add to the catalog
        memory_nodes: set of nodes which should be considered MemoryDatasets
        memory_nodes_in_catalog: whether to add MemoryDatasets to the catalog or not
    """
    mock_catalog = mock_data_catalog(
        nodes, memory_nodes, memory_nodes_in_catalog=memory_nodes_in_catalog
    )
    for node_name in nodes:
        if node_name in memory_nodes:
            assert _is_memory_dataset(mock_catalog, node_name)
        else:
            assert not _is_memory_dataset(mock_catalog, node_name)
