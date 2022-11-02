"""Stream type classes for tap-facebook-pages."""

from pathlib import Path
from typing import Any, Dict, Optional, Union, List, Iterable

# from singer_sdk import typing as th  # JSON Schema typing helpers
# from singer_sdk._singerlib import Schema
# from singer_sdk.plugin_base import PluginBase as TapBaseClass

from tap_facebook_pages.client import FacebookPagesStream

# TODO: Delete this is if not using json files for schema definition
SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")


class PagesStream(FacebookPagesStream):
    """Define custom stream."""
    name = "pages"
    path = "/{page_id}"
    primary_keys = ["id"]
    replication_key = None
    # Optionally, you may also use `schema_filepath` in place of `schema`:
    schema_filepath = SCHEMAS_DIR / "pages.json"

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Return a context dictionary for child streams."""
        return {"page_id": record["id"]}

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        params = super().get_url_params(context, next_page_token)
        params["fields"] = ",".join(self.schema["properties"].keys())
        return params


class PostsStream(FacebookPagesStream):
    """Define custom stream."""
    name = "posts"
    parent_stream_type = PagesStream
    path = "/{page_id}/published_posts"
    primary_keys = ["id"]
    replication_key = None
    # Optionally, you may also use `schema_filepath` in place of `schema`:
    schema_filepath = SCHEMAS_DIR / "posts.json"
    records_jsonpath = "$.data[*]"

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        params = super().get_url_params(context, next_page_token)
        params["access_token"] = self.page_access_tokens[context["page_id"]]
        params["fields"] = ",".join(self.schema["properties"].keys())
        return params

    # def __init__(
    #     self,
    #     tap: TapBaseClass,
    #     name: Optional[str] = None,
    #     schema: Optional[Union[Dict[str, Any], Schema]] = None,
    #     path: Optional[str] = None,
    # ) -> None:
    #     super().__init__(tap, name, schema, path)
    #     self.access_token = self.exchange_token()
