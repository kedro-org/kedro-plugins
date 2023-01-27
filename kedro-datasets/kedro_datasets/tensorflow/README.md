# TensorFlowModelDataset

``TensorflowModelDataset`` loads and saves TensorFlow models.
The underlying functionality is supported by, and passes input arguments to TensorFlow 2.X load_model and save_model methods. Only TF2 is currently supported for saving and loading, V1 requires HDF5 and serialises differently.

#### Example use:
```python
import numpy as np
import tensorflow as tf

from kedro_datasets.tensorflow import TensorFlowModelDataset

data_set = TensorFlowModelDataset("tf_model_dirname")

model = tf.keras.Model()
predictions = model.predict([...])

data_set.save(model)
loaded_model = data_set.load()

new_predictions = loaded_model.predict([...])
np.testing.assert_allclose(predictions, new_predictions, rtol=1e-6, atol=1e-6)
```

#### Example catalog.yml:
```yaml
example_tensorflow_data:
  type: tensorflow.TensorFlowModelDataset
  filepath: data/08_reporting/tf_model_dirname
  load_args:
    tf_device: "/CPU:0"  # optional
```

Contributed by (Aleks Hughes)[https://github.com/w0rdsm1th].
