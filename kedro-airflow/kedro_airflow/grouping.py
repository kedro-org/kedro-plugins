from __future__ import annotations

from collections import defaultdict

from kedro.io import DataCatalog, MemoryDataset
from kedro.pipeline.node import Node
from kedro.pipeline.pipeline import Pipeline


def _is_memory_dataset(catalog, dataset_name: str) -> bool:
    if dataset_name == "parameters" or dataset_name.startswith("params:"):
        return False

    dataset = catalog._datasets.get(dataset_name, None)
    return dataset is not None and isinstance(dataset, MemoryDataset)


def get_memory_datasets(catalog: DataCatalog, pipeline: Pipeline) -> set[str]:
    """Gather all datasets in the pipeline that are of type MemoryDataset, excluding 'parameters'."""
    return {
        dataset_name
        for dataset_name in pipeline.datasets()
        if _is_memory_dataset(catalog, dataset_name)
    }


def node_sequence_name(node_sequence: list[Node]) -> str:
    return "_".join([node.name for node in node_sequence])


def group_memory_nodes(catalog: DataCatalog, pipeline: Pipeline):
    """
    Nodes that are connected through MemoryDatasets cannot be distributed across
    multiple machines, e.g. be in different Kubernetes pods. This function
    groups nodes that are connected through MemoryDatasets in the pipeline
    together. Essentially, this computes connected components over the graph of
    nodes connected by MemoryDatasets.
    """
    # get all memory datasets in the pipeline
    memory_datasets = get_memory_datasets(catalog, pipeline)

    # Node sequences
    node_sequences = []

    # Mapping from dataset name -> node sequence index
    sequence_map = {}
    for node in pipeline.nodes:
        if all(o not in memory_datasets for o in node.inputs + node.outputs):
            # standalone node
            node_sequences.append([node])
        else:
            if all(i not in memory_datasets for i in node.inputs):
                # start of a sequence; create a new sequence and store the id
                node_sequences.append([node])
                sequence_id = len(node_sequences) - 1
            else:
                # continuation of a sequence; retrieve sequence_id
                sequence_id = None
                for i in node.inputs:
                    if i in memory_datasets:
                        assert sequence_id is None or sequence_id == sequence_map[i]
                        sequence_id = sequence_map[i]

                # Append to map
                node_sequences[sequence_id].append(node)

            # map outputs to sequence_id
            for o in node.outputs:
                if o in memory_datasets:
                    sequence_map[o] = sequence_id

    # Named node sequences
    nodes = {
        node_sequence_name(node_sequence): node_sequence
        for node_sequence in node_sequences
    }

    # Inverted mapping
    node_mapping = {
        node.name: sequence_name
        for sequence_name, node_sequence in nodes.items()
        for node in node_sequence
    }

    # Grouped dependencies
    dependencies = defaultdict(list)
    for node, parent_nodes in pipeline.node_dependencies.items():
        for parent in parent_nodes:
            parent_name = node_mapping[parent.name]
            node_name = node_mapping[node.name]
            if parent_name != node_name and (
                parent_name not in dependencies
                or node_name not in dependencies[parent_name]
            ):
                dependencies[parent_name].append(node_name)

    return nodes, dependencies
