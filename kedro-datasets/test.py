from kedro_datasets_experimental.pandas import DeltaSharingDataset

credentials = {
    "profile_file": "open-datasets.share"
    }

load_args = {
    "limit": 10,
}

dataset = DeltaSharingDataset(
    share="delta_sharing",
    schema="default",
    table="nyctaxi_2019",
    credentials=credentials,
    load_args=load_args
)

data = dataset.load()
print(data)