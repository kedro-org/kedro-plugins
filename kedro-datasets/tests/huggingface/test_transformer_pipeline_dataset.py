import pytest

from kedro_datasets.huggingface import HFTransformerPipelineDataset


@pytest.fixture
def task():
    return "fill-mask"


@pytest.fixture
def model_name():
    return "Twitter/twhin-bert-base"


class TestHFTransformerPipelineDataset:
    def test_simple_dataset_load(self, task, model_name, mocker):
        mocked_pipeline = mocker.patch(
            "kedro_datasets.huggingface.transformer_pipeline_dataset.pipeline"
        )

        dataset = HFTransformerPipelineDataset(
            task=task,
            model_name=model_name,
        )
        model = dataset.load()

        mocked_pipeline.assert_called_once_with(task, model=model_name)
        assert model is mocked_pipeline.return_value

    def test_dataset_pipeline_kwargs_load(self, task, model_name, mocker):
        mocked_pipeline = mocker.patch(
            "kedro_datasets.huggingface.transformer_pipeline_dataset.pipeline"
        )

        pipeline_kwargs = {"foo": True}
        dataset = HFTransformerPipelineDataset(
            task=task,
            model_name=model_name,
            pipeline_kwargs=pipeline_kwargs,
        )
        model = dataset.load()

        mocked_pipeline.assert_called_once_with(
            task, model=model_name, **pipeline_kwargs
        )
        assert model is mocked_pipeline.return_value

    def test_dataset_redundant_pipeline_kwargs(self, task, model_name, mocker):
        pipeline_kwargs = {"task": "redundant"}
        with pytest.warns(
            UserWarning,
            match="Specifying 'task' or 'model' in 'pipeline_kwargs' is not allowed",
        ):
            HFTransformerPipelineDataset(
                task=task,
                model_name=model_name,
                pipeline_kwargs=pipeline_kwargs,
            )

    def test_dataset_incomplete_init(self):
        with pytest.raises(
            ValueError, match="At least 'task' or 'model_name' are needed"
        ):
            HFTransformerPipelineDataset()
