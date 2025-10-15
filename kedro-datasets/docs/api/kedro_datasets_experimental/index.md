# kedro_datasets_experimental

::: kedro_datasets_experimental

## Dataset Classes

Name | Description
-----|------------
[databricks.ExternalTableDataset](databricks.ExternalTableDataset.md) | ``ExternalTableDataset`` implementation to access external tables in Databricks.
[langchain.ChatAnthropicDataset](langchain.ChatAnthropicDataset.md) | ``ChatAnthropicDataset`` loads a ChatAnthropic `langchain` model.
[langchain.ChatCohereDataset](langchain.ChatCohereDataset.md) | ``ChatCohereDataset`` loads a ChatCohere `langchain` model.
[langchain.ChatOpenAIDataset](langchain.ChatOpenAIDataset.md) | OpenAI dataset used to access credentials at runtime.
[langchain.OpenAIEmbeddingsDataset](langchain.OpenAIEmbeddingsDataset.md) | ``OpenAIEmbeddingsDataset`` loads a OpenAIEmbeddings `langchain` model.
[langchain.LangChainPromptDataset](langchain.LangChainPromptDataset.md) | ``LangChainPromptDataset`` loads a `langchain` prompt template.
[langfuse.LangfusePromptDataset](langfuse.LangfusePromptDataset.md) | ``LangfusePromptDataset`` provides a seamless integration between local prompt files (JSON/YAML) and Langfuse prompt management, supporting version control, labeling, and different synchronization policies.
[langfuse.LangfuseTraceDataset](langfuse.LangfuseTraceDataset.md) | ``LangfuseTraceDataset`` provides Langfuse tracing clients for LLM observability and monitoring.
[netcdf.NetCDFDataset](netcdf.NetCDFDataset.md) | ``NetCDFDataset`` loads/saves data from/to a NetCDF file using an underlying filesystem (e.g.: local, S3, GCS). It uses xarray to handle the NetCDF file.
[pypdf.PDFDataset](pypdf.PDFDataset.md) | ``PDFDataset`` loads data from PDF files using pypdf to extract text from pages. Read-only dataset.
[polars.PolarsDatabaseDataset](polars.PolarsDatabaseDataset.md) | ``PolarsDatabaseDataset`` implementation to access databases as Polars DataFrames. It supports reading from a SQL query and writing to a database table.
[prophet.ProphetModelDataset](prophet.ProphetModelDataset.md) | ``ProphetModelDataset`` loads/saves Facebook Prophet models to a JSON file using an underlying filesystem (e.g., local, S3, GCS). It uses Prophet's built-in serialisation to handle the JSON file.
[pytorch.PyTorchDataset](pytorch.PyTorchDataset.md) | ``PyTorchDataset`` loads and saves PyTorch models' `state_dict` using PyTorch's recommended zipfile serialization protocol. To avoid security issues with Pickle.
[rioxarray.GeoTIFFDataset](rioxarray.GeoTIFFDataset.md) | Loads and saves raster data files as xarray DataArrays. Supports single and multiband GeoTIFFs with CRS validation.
[safetensors.SafetensorsDataset](safetensors.SafetensorsDataset.md) | Loads and saves data using the SafeTensors library with support for multiple backends like numpy and torch.
[video.VideoDataset](video.VideoDataset.md) | Loads and saves video data as a sequence of images using OpenCV, supporting various codecs and formats.
