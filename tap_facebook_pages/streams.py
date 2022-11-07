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

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Return a context dictionary for child streams."""
        return {
            "page_id": context["page_id"],
            "post_id": record["id"],
        }

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        params = super().get_url_params(context, next_page_token)
        params["access_token"] = self.page_access_tokens[context["page_id"]]
        params["fields"] = ",".join(self.schema["properties"].keys())
        return params


class VideoStream(FacebookPagesStream):
    name = "videos"
    parent_stream_type = PagesStream
    path = "/{page_id}/videos"
    primary_keys = ["id"]
    replication_key = None
    schema_filepath = SCHEMAS_DIR / "videos.json"
    records_jsonpath = "$.data[*]"

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        params = super().get_url_params(context, next_page_token)
        params["access_token"] = self.page_access_tokens[context["page_id"]]
        params["fields"] = ",".join(self.schema["properties"].keys())
        return params


# class PostAttachmentsStream(FacebookPagesStream):
#     name = "post_attachments"
#     parent_stream_type = PostsStream
#     path = "/{post_id}/attachments"
#     primary_keys = ["id"]
#     replication_key = None
#     schema_filepath = SCHEMAS_DIR / "post_attachments.json"
#     records_jsonpath = "$.data[*]"


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
        return params

    def post_process(self, row: dict, context: Optional[dict]) -> dict:
        """As needed, append or transform raw data to match expected structure."""
        for val in row["values"]:
            if isinstance(val["value"], int):
                val["value"] = {"total": val["value"]}
        return row


class PostInsightsStream(FacebookPagesStream):
    """Base class for Page Insights streams"""
    name = None
    parent_stream_type = PostsStream
    path = "/{post_id}/insights"
    primary_keys = ["id"]
    replication_key = None
    schema_filepath = SCHEMAS_DIR / "page_insights.json"  # TODO: update
    records_jsonpath = "$.data[*]"
    metrics: List[str] = None

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        params = super().get_url_params(context, next_page_token)
        params["access_token"] = self.page_access_tokens[context["page_id"]]
        params["metric"] = ",".join(self.metrics)
        return params

    def post_process(self, row: dict, context: Optional[dict]) -> dict:
        """As needed, append or transform raw data to match expected structure."""
        for val in row["values"]:
            if isinstance(val["value"], int):
                val["value"] = {"total": val["value"]}
        return row


class PageEngagementInsightsStream(PageInsightsStream):
    """https://developers.facebook.com/docs/graph-api/reference/insights#page-engagement"""
    name = "page_engagement_insights"
    metrics = [
        "page_engaged_users",
        "page_post_engagements",
        "page_consumptions",
        "page_consumptions_unique",
        "page_consumptions_by_consumption_type",
        "page_consumptions_by_consumption_type_unique",
        "page_places_checkin_total",
        "page_places_checkin_total_unique",
        "page_places_checkin_mobile",
        "page_places_checkin_mobile_unique",
        "page_places_checkins_by_age_gender",
        "page_places_checkins_by_locale",
        "page_places_checkins_by_country",
        "page_negative_feedback",
        "page_negative_feedback_unique",
        "page_negative_feedback_by_type",
        "page_negative_feedback_by_type_unique",
        "page_positive_feedback_by_type",
        "page_positive_feedback_by_type_unique",
        "page_fans_online",
        "page_fans_online_per_day",
        "page_fan_adds_by_paid_non_paid_unique",
    ]


class PageImpressionsInsightsStream(PageInsightsStream):
    """https://developers.facebook.com/docs/graph-api/reference/insights#page-impressions"""
    name = "page_impressions_insights"
    metrics = [
        "page_impressions",
        "page_impressions_unique",
        "page_impressions_paid",
        "page_impressions_paid_unique",
        "page_impressions_organic",
        "page_impressions_organic_unique",
        "page_impressions_viral",
        "page_impressions_viral_unique",
        "page_impressions_nonviral",
        "page_impressions_nonviral_unique",
        "page_impressions_by_story_type",
        "page_impressions_by_story_type_unique",
        "page_impressions_by_city_unique",
        "page_impressions_by_country_unique",
        "page_impressions_by_locale_unique",
        "page_impressions_by_age_gender_unique",
        "page_impressions_frequency_distribution",
        "page_impressions_viral_frequency_distribution",
    ]


class PagePostsInsightsStream(PageInsightsStream):
    """https://developers.facebook.com/docs/graph-api/reference/insights#page-posts"""
    name = "page_posts_insights"
    metrics = [
        "page_posts_impressions",
        "page_posts_impressions_unique",
        "page_posts_impressions_paid",
        "page_posts_impressions_paid_unique",
        "page_posts_impressions_organic",
        "page_posts_impressions_organic_unique",
        "page_posts_served_impressions_organic_unique",
        "page_posts_impressions_viral",
        "page_posts_impressions_viral_unique",
        "page_posts_impressions_nonviral",
        "page_posts_impressions_nonviral_unique",
        "page_posts_impressions_frequency_distribution",
    ]


class PagePostEngagementInsightsStream(PostInsightsStream):
    """https://developers.facebook.com/docs/graph-api/reference/insights#page-post-engagement"""
    name = "page_post_engagement_insights"
    metrics = [
        "post_engaged_users",
        "post_negative_feedback",
        "post_negative_feedback_unique",
        "post_negative_feedback_by_type",
        "post_negative_feedback_by_type_unique",
        "post_engaged_fan",
        "post_clicks",
        "post_clicks_unique",
        "post_clicks_by_type",
        "post_clicks_by_type_unique",
    ]


class PagePostImpressionsInsightsStream(PostInsightsStream):
    """https://developers.facebook.com/docs/graph-api/reference/insights#page-post-impressions"""
    name = "page_post_impressions_insights"
    metrics = [
        "post_impressions",
        "post_impressions_unique",
        "post_impressions_paid",
        "post_impressions_paid_unique",
        "post_impressions_fan",
        "post_impressions_fan_unique",
        "post_impressions_fan_paid",
        "post_impressions_fan_paid_unique",
        "post_impressions_organic",
        "post_impressions_organic_unique",
        "post_impressions_viral",
        "post_impressions_viral_unique",
        "post_impressions_nonviral",
        "post_impressions_nonviral_unique",
        "post_impressions_by_story_type",
        "post_impressions_by_story_type_unique",
    ]


class PageVideoViewsInsightsStream(PageInsightsStream):
    """https://developers.facebook.com/docs/graph-api/reference/insights#videoviews"""
    name = "page_video_views_insights"
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


class PageVideoPostsInsightsStream(PostInsightsStream):
    """https://developers.facebook.com/docs/graph-api/reference/insights#page-video-posts"""
    name = "page_video_post_insights"
    metrics = [
        "post_video_avg_time_watched",
        "post_video_complete_views_organic",
        "post_video_complete_views_organic_unique",
        "post_video_complete_views_paid",
        "post_video_complete_views_paid_unique",
        "post_video_retention_graph",
        "post_video_retention_graph_clicked_to_play",
        "post_video_retention_graph_autoplayed",
        "post_video_views_organic",
        "post_video_views_organic_unique",
        "post_video_views_paid",
        "post_video_views_paid_unique",
        "post_video_length",
        "post_video_views",
        "post_video_views_unique",
        "post_video_views_autoplayed",
        "post_video_views_clicked_to_play",
        "post_video_views_15s",
        "post_video_views_60s_excludes_shorter",
        "post_video_views_10s",
        "post_video_views_10s_unique",
        "post_video_views_10s_autoplayed",
        "post_video_views_10s_clicked_to_play",
        "post_video_views_10s_organic",
        "post_video_views_10s_paid",
        "post_video_views_10s_sound_on",
        "post_video_views_sound_on",
        "post_video_view_time",
        "post_video_view_time_organic",
    ]


class PageVideoAdBreaksInsightsStream(PageInsightsStream):
    """https://developers.facebook.com/docs/graph-api/reference/insights#video-ad-breaks"""
    metrics = [
        "page_daily_video_ad_break_ad_impressions_by_crosspost_status",
        "page_daily_video_ad_break_cpm_by_crosspost_status",
        "page_daily_video_ad_break_earnings_by_crosspost_status",
        "post_video_ad_break_ad_impressions",
        "post_video_ad_break_earnings",
        "post_video_ad_break_ad_cpm",
    ]
