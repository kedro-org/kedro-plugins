"""
Cohere dataset definition.
"""

from typing import Any, Dict, NoReturn

from cohere import AsyncClient, Client
from kedro.io import AbstractDataset, DatasetError
from langchain.llms import Cohere


class CohereDataset(AbstractDataset[None, Cohere]):
    """``CohereDataset`` loads a Cohere `langchain <https://python.langchain.com/>`_ model.

    Example usage for the :doc:`YAML API <kedro:data/data_catalog_yaml_examples>`:

    catalog.yml:

    .. code-block:: yaml
       command:
         type: langchain.CohereDataset
         kwargs:
           model: "command"
           temperature: 0.0
         credentials: cohere


    credentials.yml:

    .. code-block:: yaml
       cohere:
         cohere_api_url: <cohere-api-base>
         cohere_api_key: <cohere-api-key>

    Example usage for the
    `Python API <https://kedro.readthedocs.io/en/stable/data/\
    advanced_data_catalog_usage.html>`_:

    .. code-block:: python
        >>> from kedro_datasets.langchain import CohereDataset
        >>> llm = CohereDataset(
        ...     credentials={
        ...         "cohere_api_url": "https://cohere.prod.ai-gateway.quantumblack.com/105f9053-ca49-4e42-8dc6-3d69b57fa4c4/",
        ...         "cohere_api_key": "eyJhbGciOiJSUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICJhZXNKN2kxNGNidnVuTU40MTJrOU5yZ2ROeENhTlJudTNPbC1TU08ycFlJIn0.eyJleHAiOjE3MDExMDA0NjYsImlhdCI6MTcwMTA5ODY2NiwiYXV0aF90aW1lIjoxNzAxMDk4NjY1LCJqdGkiOiIzZGY4M2JmMS01Y2I3LTQ2OWQtOTA0My1mNWE1ZTE0YmUxYWMiLCJpc3MiOiJodHRwczovL2F1dGgubWNraW5zZXkuaWQvYXV0aC9yZWFsbXMvciIsImF1ZCI6ImJjZDIzNzI4LTNkMjctNDQ3Yy1hMGE5LWVhY2FmMzkzYTZmNSIsInN1YiI6IjViYjk3NWNhLWM2MjUtNGJiNS05MGQ3LTBjNWQzY2MxMTVhMSIsInR5cCI6IklEIiwiYXpwIjoiYmNkMjM3MjgtM2QyNy00NDdjLWEwYTktZWFjYWYzOTNhNmY1Iiwibm9uY2UiOiI2Tkt0OVF0YkFRN05Ba1UteDN2X21LZ1B4UWdVSDNKN2xWWDRnQzFKM2J3Iiwic2Vzc2lvbl9zdGF0ZSI6Ijg3MmRhZDM4LWM2M2MtNDU5Ni1hYTY3LWY3ZjNiZGE4OWNiOSIsImF0X2hhc2giOiJnT2Z6RVU3SjdyTlItQnM5ei1fZVZnIiwibmFtZSI6IklhbiBXaGFsZW4iLCJnaXZlbl9uYW1lIjoiSWFuIiwiZmFtaWx5X25hbWUiOiJXaGFsZW4iLCJwcmVmZXJyZWRfdXNlcm5hbWUiOiI3OTYwZTk2YjdkYmM3YTEzIiwiZW1haWwiOiJJYW5fV2hhbGVuQG1ja2luc2V5LmNvbSIsImFjciI6IjEiLCJzaWQiOiI4NzJkYWQzOC1jNjNjLTQ1OTYtYWE2Ny1mN2YzYmRhODljYjkiLCJlbWFpbF92ZXJpZmllZCI6dHJ1ZSwiZm1ubyI6IjE4MjAwNiIsImdyb3VwcyI6WyI4MGQ1YzJhZi1kYWM0LTRiOTAtYTY0NC03MTBlYmVhMjQzZDUiLCIzZDgzYzU5NC03NjA2LTRlOGEtODIxMy1mODU1NTc2MTlkZTAiLCJBbGwgRmlybSBVc2VycyIsIjdjMzkzMjAyLTNhYjEtNDA4ZS04NTg3LTIyOTUxNDQwOTE3OCIsIjA3MDQxZGE0LWNlOTUtNGQyYy05ZDU0LThjNTViMjliMDVmZSIsIjEwNWY5MDUzLWNhNDktNGU0Mi04ZGM2LTNkNjliNTdmYTRjNCIsIjM4M2RkNTA5LTBmMjYtNDMwNS1hYmY3LWJmN2ZiN2JmZTI4ZSIsImQwNGViNzM5LTRjNzgtNGVjZi05ZDhkLWIxMDM4NGY1MjM4MyJdfQ.DuYKT4vsaq28ReEDYblSMHc_CO-GwJlHrdadg05py-nibbhVMPR-SR1N06awTbhqrvB-G0RjE2vAqiEXhk40ArmLhuEcRlTg-yBZtEr52A1SHWZmzfvZOx5pRZkeLBFoQytpf076juPx_ah2vE8fK2NlMI3pgxXwbN-cXi4dTQ3U6cJCHUsaSOccklgekJlqhRfkGsmx-_RawtHXRro4o1AV6Bc-Y3-HgWrmjbGRnaZQ--Eo887wnqdguEXtS25SnI92CK5y1nVED261cRVOTx8DFheA2c_SAkZh-qi-gI6TaAevdMCUHJ2QiIHwyliOFzl4_uXLjhiR6eng1XQMVQ",
        ...     },
        ...     kwargs={
        ...         "model": "command",
        ...         "temperature": 0,
        ...     }
        ... ).load()
        >>>
        >>> # See: https://python.langchain.com/docs/integrations/llms/cohere
        >>> llm("Hello world!")
    """

    def __init__(self, credentials: Dict[str, str], kwargs: Dict[str, Any] = None):
        """Constructor.

        Args:
            credentials: must contain `cohere_api_url` and `cohere_api_key`.
            kwargs: keyword arguments passed to the underlying constructor.
        """
        self.cohere_api_url = credentials["cohere_api_url"]
        self.cohere_api_key = credentials["cohere_api_key"]
        self.kwargs = kwargs or {}

    def _describe(self) -> dict[str, Any]:
        return {**self.kwargs}

    def _save(self, data: None) -> NoReturn:
        raise DatasetError(f"{self.__class__.__name__} is a read only data set type")

    def _load(self) -> Cohere:
        llm = Cohere(cohere_api_key="_", **self.kwargs)

        client_kwargs = {
            "api_key": self.cohere_api_key,
            "api_url": self.cohere_api_url,
        }
        llm.client = Client(**client_kwargs, client_name=llm.user_agent)
        llm.async_client = AsyncClient(**client_kwargs, client_name=llm.user_agent)

        return llm
