from __future__ import annotations

from typing import Any

from kedro.io import DataCatalog
from kedro.pipeline.node import Node
from kedro.pipeline.pipeline import Pipeline

try:
    from kedro.io import CatalogProtocol
except ImportError:  # pragma: no cover
    pass


def _is_memory_dataset(catalog, dataset_name: str) -> bool:
    if dataset_name not in catalog:
        return True
    return False


def get_memory_datasets(
    catalog: CatalogProtocol[Any] | DataCatalog, pipeline: Pipeline
) -> set[str]:
    """Gather all datasets in the pipeline that are of type MemoryDataset, excluding 'parameters'."""
    return {
        dataset_name
        for dataset_name in pipeline.datasets()
        if _is_memory_dataset(catalog, dataset_name)
    }


def create_adjacency_list(
    catalog: CatalogProtocol[Any] | DataCatalog, pipeline: Pipeline
) -> tuple[dict[str, set], dict[str, set]]:
    """
    Builds adjacency list (adj_list) to search connected components - undirected graph,
    and adjacency list (parent_to_children) to retrieve connections between new components
    using on initial kedro topological sort - directed graph.
    """
    memory_datasets = get_memory_datasets(catalog, pipeline)

    adj_list: dict[str, set] = {node.name: set() for node in pipeline.nodes}
    parent_to_children: dict[str, set] = {node.name: set() for node in pipeline.nodes}
    output_to_node = {
        node_output: node for node in pipeline.nodes for node_output in node.outputs
    }

    for node in pipeline.nodes:
        for node_input in node.inputs:
            if node_input in output_to_node:
                parent_to_children[output_to_node[node_input].name].add(node.name)
                if node_input in memory_datasets:
                    adj_list[node.name].add(output_to_node[node_input].name)
                    adj_list[output_to_node[node_input].name].add(node.name)

    return adj_list, parent_to_children


def group_memory_nodes(
    catalog: CatalogProtocol[Any] | DataCatalog, pipeline: Pipeline
) -> tuple[dict[str, list[Node]], dict[str, list[str]]]:
    """
    Nodes that are connected through MemoryDatasets cannot be distributed across
    multiple machines, e.g. be in different Kubernetes pods. This function
    groups nodes that are connected through MemoryDatasets in the pipeline
    together. Essentially, this computes connected components over the graph of
    nodes connected by MemoryDatasets.
    """
    # Creating adjacency list
    adj_list, parent_to_children = create_adjacency_list(catalog, pipeline)

    name_to_node = {node.name: node for node in pipeline.nodes}
    con_components: dict[str, int] = {node.name: -1 for node in pipeline.nodes}

    # Searching connected components
    def dfs(cur_node_name: str, component: int) -> None:
        if con_components[cur_node_name] != -1:
            return

        con_components[cur_node_name] = component
        for next_node_name in adj_list[cur_node_name]:
            dfs(next_node_name, component)

    cur_component = 0
    for node_name in adj_list.keys():
        if con_components[node_name] == -1:
            dfs(node_name, cur_component)
            cur_component += 1

    # Joining nodes based on found connected components
    groups: list[list[str]] = [[] for _ in range(cur_component)]
    for node_name, component in con_components.items():
        groups[component].append(node_name)

    group_to_seq: dict[str, list[Node]] = {}
    old_name_to_group = {}
    for group in groups:
        group_name = "_".join(group)
        group_to_seq[group_name] = [name_to_node[node_name] for node_name in group]
        for node_name in group:
            old_name_to_group[node_name] = group_name

    # Retrieving dependencies between joined nodes based on initial topological sort
    group_dependencies: dict[str, list[str]] = {}
    for parent, children in parent_to_children.items():
        if not children:
            continue
        new_name_parent = old_name_to_group[parent]
        if new_name_parent not in group_dependencies:
            group_dependencies[new_name_parent] = []
        for child in children:
            new_name_child = old_name_to_group[child]
            if new_name_parent != new_name_child:
                group_dependencies[new_name_parent].append(new_name_child)

    return group_to_seq, group_dependencies
