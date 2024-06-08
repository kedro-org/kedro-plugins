from prophet_dataset import ProphetModelDataset
from prophet import Prophet
import pandas as pd

# Sample data
df = pd.DataFrame({
    'ds': pd.date_range(start='2020-01-01', periods=100),
    'y': range(100)
})

# Fit the model
model = Prophet()
model.fit(df)

# Save the model
dataset = ProphetModelDataset(filepath="path/to/model.json")
dataset.save(model)

# Load the model
reloaded_model = dataset.load()
assert isinstance(reloaded_model, Prophet)

