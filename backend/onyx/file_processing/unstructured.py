import os
from typing import Any
from typing import cast
from typing import IO
from typing import TYPE_CHECKING

from onyx.configs.constants import KV_UNSTRUCTURED_API_KEY
from onyx.key_value_store.factory import get_kv_store
from onyx.key_value_store.interface import KvKeyNotFoundError
from onyx.utils.logger import setup_logger

if TYPE_CHECKING:
    from unstructured_client.models import operations  # type: ignore


logger = setup_logger()

UNSTRUCTURED_SERVER_URL_ENV = "UNSTRUCTURED_API_URL"


def get_unstructured_api_key() -> str | None:
    kv_store = get_kv_store()
    try:
        return cast(str, kv_store.load(KV_UNSTRUCTURED_API_KEY))
    except KvKeyNotFoundError:
        return None


def update_unstructured_api_key(api_key: str) -> None:
    kv_store = get_kv_store()
    kv_store.store(KV_UNSTRUCTURED_API_KEY, api_key)


def delete_unstructured_api_key() -> None:
    kv_store = get_kv_store()
    kv_store.delete(KV_UNSTRUCTURED_API_KEY)


def get_unstructured_server_url() -> str | None:
    """
    Returns a custom Unstructured API base URL when provided via environment variable.
    """
    server_url = os.environ.get(UNSTRUCTURED_SERVER_URL_ENV, "").strip()
    return server_url or None


def _sdk_partition_request(
    file: IO[Any], file_name: str, **kwargs: Any
) -> "operations.PartitionRequest":
    from unstructured_client.models import operations  # type: ignore
    from unstructured_client.models import shared

    file.seek(0, 0)
    try:
        request = operations.PartitionRequest(
            partition_parameters=shared.PartitionParameters(
                files=shared.Files(content=file.read(), file_name=file_name),
                **kwargs,
            ),
        )
        return request
    except Exception as e:
        logger.error(f"Error creating partition request for file {file_name}: {str(e)}")
        raise


def unstructured_to_text(file: IO[Any], file_name: str) -> str:
    from unstructured.staging.base import dict_to_elements
    from unstructured_client import UnstructuredClient  # type: ignore

    logger.debug(f"Starting to read file: {file_name}")
    req = _sdk_partition_request(file, file_name, strategy="fast")

    client_kwargs: dict[str, Any] = {}
    api_key = get_unstructured_api_key()
    if api_key:
        client_kwargs["api_key_auth"] = api_key

    server_url = get_unstructured_server_url()
    if server_url:
        client_kwargs["server_url"] = server_url
        logger.debug(f"Using custom Unstructured server URL: {server_url}")

    unstructured_client = UnstructuredClient(**client_kwargs)  # type: ignore[arg-type]

    response = unstructured_client.general.partition(req)  # type: ignore
    elements = dict_to_elements(response.elements)

    if response.status_code != 200:
        err = f"Received unexpected status code {response.status_code} from Unstructured API."
        logger.error(err)
        raise ValueError(err)

    return "\n\n".join(str(el) for el in elements)
