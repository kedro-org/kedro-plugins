from kedro.pipeline import Pipeline, node, pipeline


def one(x):
    return "dummy"


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline([node(one, "dummy", "dummy_output")])
