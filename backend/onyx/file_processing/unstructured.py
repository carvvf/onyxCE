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
UNSTRUCTURED_STRATEGY_ENV = "UNSTRUCTURED_STRATEGY"
UNSTRUCTURED_HI_RES_MODEL_ENV = "UNSTRUCTURED_HI_RES_MODEL_NAME"
VALID_UNSTRUCTURED_STRATEGIES = {"fast", "hi_res", "auto", "ocr_only"}


def is_unstructured_hi_res_strategy_enabled() -> bool:
    strategy = os.environ.get(UNSTRUCTURED_STRATEGY_ENV, "").strip().lower()
    return strategy == "hi_res"


def _get_bool_env(var_name: str, default: bool) -> bool:
    value = os.environ.get(var_name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes"}


def _get_list_env(var_name: str) -> list[str] | None:
    raw_value = os.environ.get(var_name)
    if not raw_value:
        return None
    return [item.strip() for item in raw_value.split(",") if item.strip()]


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


def _get_partition_params() -> dict[str, Any]:
    strategy = os.environ.get(UNSTRUCTURED_STRATEGY_ENV, "").strip().lower()
    if not strategy:
        strategy = "fast"
    elif strategy not in VALID_UNSTRUCTURED_STRATEGIES:
        logger.warning(
            "Invalid UNSTRUCTURED_STRATEGY '%s'. Falling back to 'fast'.", strategy
        )
        strategy = "fast"

    params: dict[str, Any] = {"strategy": strategy}

    # Request metadata-rich output for downstream processing by default.
    if _get_bool_env("UNSTRUCTURED_COORDINATES", True):
        params["coordinates"] = True

    params["include_page_breaks"] = _get_bool_env("UNSTRUCTURED_INCLUDE_PAGE_BREAKS", False)
    params["unique_element_ids"] = _get_bool_env("UNSTRUCTURED_UNIQUE_ELEMENT_IDS", True)

    if languages := os.environ.get("UNSTRUCTURED_LANGUAGES"):
        params["languages"] = [lang.strip() for lang in languages.split(",") if lang.strip()]

    params["multipage_sections"] = _get_bool_env("UNSTRUCTURED_MULTIPAGE_SECTIONS", True)
    if combine_under := os.environ.get("UNSTRUCTURED_COMBINE_UNDER_N_CHARS"):
        params["combine_under_n_chars"] = int(combine_under)
    if max_chars := os.environ.get("UNSTRUCTURED_MAX_CHARACTERS"):
        params["max_characters"] = int(max_chars)
    if new_after := os.environ.get("UNSTRUCTURED_NEW_AFTER_N_CHARS"):
        params["new_after_n_chars"] = int(new_after)
    if overlap := os.environ.get("UNSTRUCTURED_OVERLAP"):
        params["overlap"] = int(overlap)
    params["overlap_all"] = _get_bool_env("UNSTRUCTURED_OVERLAP_ALL", False)

    if (include_slide_notes := os.environ.get("UNSTRUCTURED_INCLUDE_SLIDE_NOTES")) is not None:
        params["include_slide_notes"] = include_slide_notes.strip().lower() in {"1", "true", "yes"}

    params["pdf_infer_table_structure"] = _get_bool_env("UNSTRUCTURED_PDF_INFER_TABLE_STRUCTURE", True)
    if skip_types := _get_list_env("UNSTRUCTURED_SKIP_INFER_TABLE_TYPES"):
        params["skip_infer_table_types"] = skip_types

    if extract_types := _get_list_env("UNSTRUCTURED_EXTRACT_IMAGE_BLOCK_TYPES"):
        params["extract_image_block_types"] = extract_types

    hi_res_model = os.environ.get(UNSTRUCTURED_HI_RES_MODEL_ENV, "").strip()
    if hi_res_model:
        if strategy == "hi_res":
            params["hi_res_model_name"] = hi_res_model
        else:
            logger.warning(
                "UNSTRUCTURED_HI_RES_MODEL_NAME is set but strategy is '%s'; ignoring hi_res model.",
                strategy,
            )

    logger.debug(
        "Using Unstructured partition params: strategy='%s'%s",
        strategy,
        f", hi_res_model_name='{params.get('hi_res_model_name')}'"
        if "hi_res_model_name" in params
        else "",
    )
    return params


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
    partition_params = _get_partition_params()
    req = _sdk_partition_request(file, file_name, **partition_params)

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
