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
    schema_filepath = SCHEMAS_DIR / "posts.json"
    records_jsonpath = "$.data[*]"

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        params = super().get_url_params(context, next_page_token)
        params["access_token"] = self.page_access_tokens[context["page_id"]]
        params["fields"] = ",".join(self.schema["properties"].keys())
        return params


class PageInsightsStream(FacebookPagesStream):
    """Base class for Page Insights streams"""
    name = None
    parent_stream_type = PagesStream
    path = "/{page_id}/insights"
    primary_keys = ["id"]
    replication_key = None
    schema_filepath = SCHEMAS_DIR / "page_insights.json"
    records_jsonpath = "$.data[*]"
    metrics: List[str] = None

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        params = super().get_url_params(context, next_page_token)
        params["access_token"] = self.page_access_tokens[context["page_id"]]
        params["metric"] = ",".join(self.metrics)
        # params.update({"metric": ",".join(self.metrics)})
        return params

    def post_process(self, row: dict, context: Optional[dict]) -> dict:
        """As needed, append or transform raw data to match expected structure."""
        for val in row["values"]:
            if isinstance(val["value"], int):
                val["value"] = {"total": val["value"]}
        return row


class PageInsightsVideoViewsStream(PageInsightsStream):
    """Define custom stream."""
    name = "page_insights_video_views"
    metrics = [
        "page_video_views",
        "page_video_views_paid",
        "page_video_views_organic",
        "page_video_views_by_paid_non_paid",
        "page_video_views_autoplayed",
        "page_video_views_click_to_play",
        "page_video_views_unique",
        "page_video_repeat_views",
        "page_video_complete_views_30s",
        "page_video_complete_views_30s_paid",
        "page_video_complete_views_30s_organic",
        "page_video_complete_views_30s_autoplayed",
        "page_video_complete_views_30s_click_to_play",
        "page_video_complete_views_30s_unique",
        "page_video_complete_views_30s_repeat_views",
        "post_video_complete_views_30s_autoplayed",
        "post_video_complete_views_30s_clicked_to_play",
        "post_video_complete_views_30s_organic",
        "post_video_complete_views_30s_paid",
        "post_video_complete_views_30s_unique",
    ]
