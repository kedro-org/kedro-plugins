from __future__ import annotations

from typing import Tuple

from kedro.io import DataCatalog, MemoryDataset
from kedro.pipeline.node import Node
from kedro.pipeline.pipeline import Pipeline


def _is_memory_dataset(catalog, dataset_name: str) -> bool:
    if dataset_name not in catalog:
        return True
    return False


def get_memory_datasets(catalog: DataCatalog, pipeline: Pipeline) -> set[str]:
    """Gather all datasets in the pipeline that are of type MemoryDataset, excluding 'parameters'."""
    return {
        dataset_name
        for dataset_name in pipeline.datasets()
        if _is_memory_dataset(catalog, dataset_name)
    }


def group_memory_nodes(
    catalog: DataCatalog, pipeline: Pipeline
) -> Tuple[dict[str, list[Node]], dict[str, list[str]]]:
    """
    Nodes that are connected through MemoryDatasets cannot be distributed across
    multiple machines, e.g. be in different Kubernetes pods. This function
    groups nodes that are connected through MemoryDatasets in the pipeline
    together. Essentially, this computes connected components over the graph of
    nodes connected by MemoryDatasets.
    """
    memory_datasets = get_memory_datasets(catalog, pipeline)

    aj_matrix = {node.name: set() for node in pipeline.nodes}
    parents = {node.name: set() for node in pipeline.nodes}
    name_to_node = {node.name: node for node in pipeline.nodes}
    output_to_node = {
        node_output: node for node in pipeline.nodes for node_output in node.outputs
    }

    for node in pipeline.nodes:
        for node_input in node.inputs:
            if node_input in output_to_node:
                if node_input in memory_datasets:
                    aj_matrix[node.name].add(output_to_node[node_input].name)
                    aj_matrix[output_to_node[node_input].name].add(node.name)
                parents[output_to_node[node_input].name].add(node.name)

    con_components = {node.name: None for node in pipeline.nodes}

    def dfs(cur_node_name: str, component: int) -> None:
        if con_components[cur_node_name] is not None:
            return

        con_components[cur_node_name] = component
        for next_node_name in aj_matrix[cur_node_name]:
            dfs(next_node_name, component)

    cur_component = 0
    for node_name in aj_matrix.keys():
        if con_components[node_name] is None:
            dfs(node_name, cur_component)
            cur_component += 1

    groups = [[] for _ in range(cur_component)]
    for node_name, component in con_components.items():
        groups[component].append(node_name)

    group_to_seq = {}
    old_name_to_group = {}
    for group in groups:
        group_name = "_".join(group)
        group_to_seq[group_name] = [name_to_node[node_name] for node_name in group]
        for node_name in group:
            old_name_to_group[node_name] = group_name

    group_dependencies = {}
    for parent, children in parents.items():
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
