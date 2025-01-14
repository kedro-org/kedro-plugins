from kedro.pipeline import Pipeline, node, pipeline


def one():
    return "dummy"


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline([node(one, [], "dummy_output")])
