"""Stream type classes for tap-facebook-pages."""

import requests
from pathlib import Path
from typing import Any, Dict, Optional, Union, List, Iterable

import pendulum
from google.cloud import bigquery

from tap_facebook_pages.client import FacebookPagesStream


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
        params["access_token"] = self.page_access_tokens[context["page_id"]]
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


class AllPostsStream(FacebookPagesStream):
    name = "all_posts"
    parent_stream_type = PagesStream
    # path = "/{page_id}/published_posts"
    primary_keys = ["id"]
    replication_key = None
    schema_filepath = SCHEMAS_DIR / "posts.json"
    records_jsonpath = "$"

    def request_records(self, context: Optional[dict]) -> Iterable[dict]:
        # TODO: take table name from config
        posts_query = """
            select distinct post_id as id, created_time
            from `g9-data-warehouse-prod.facebook_posts.most_recent`
            where 
                split(post_id, '_')[safe_offset(0)] = @page_id
                and date(created_time) >= date_sub(current_date, interval @insights_lookback_months month)
        """
        self.logger.info(f"Executing query: {posts_query}")
        bigquery_client = bigquery.Client()
        query_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("page_id", "STRING", context["page_id"]),
                bigquery.ScalarQueryParameter(
                    "insights_lookback_months",
                    "INTEGER",
                    self.config.get("insights_lookback_months"),
                ),
            ]
        )
        for row in bigquery_client.query(posts_query, job_config=query_config).result():
            yield dict(row)

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        return {
            "page_id": context["page_id"],
            "post_id": record["id"],
            "created_time": record["created_time"].strftime("%Y-%m-%d"),
        }


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


class InsightsStream(FacebookPagesStream):
    """
    Base class for all Insights streams. They all return InsightsResult objects as described
    [here](https://developers.facebook.com/docs/graph-api/reference/insights-result/)
    """
    name = None
    primary_keys = ["id"]
    replication_key = "end_time"
    schema_filepath = SCHEMAS_DIR / "post_insights.json"  # TODO: different schemas for page/post insights
    records_jsonpath = "$.data[*]"
    metrics: List[str] = None

    def get_next_page_token(
        self, response: requests.Response, previous_token: Optional[Any]
    ) -> Optional[Any]:
        return None

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        resp_json = response.json()
        if "data" not in resp_json:
            self.logger.warning(f"No data found: {resp_json}")
            return
        for row in resp_json["data"]:
            base_item = {
                "name": row["name"],
                "period": row["period"],
                "title": row["title"],
                "id": row["id"],
            }
            if "values" in row:
                for values in row["values"]:
                    if values.get("end_time"):  # non lifetime
                        end_time = pendulum.parse(values.get("end_time")).to_datetime_string()
                    else:  # lifetime
                        # setting end_time to some old date lets use still use it at the replication_key
                        # but without actually updating the state progress marker
                        end_time = pendulum.today().subtract(years=2).to_datetime_string()
                    if isinstance(values["value"], dict):
                        for key, value in values["value"].items():
                            if isinstance(value, dict):
                                for k, v in value.items():
                                    item = {
                                        "context": f"{key} > {k}",
                                        "value": float(v),
                                        "end_time": end_time
                                    }
                                    item.update(base_item)
                                    yield item
                            else:
                                item = {
                                    "context": key,
                                    "value": float(value),
                                    "end_time": end_time
                                }
                                item.update(base_item)
                                yield item
                    else:
                        values["end_time"] = end_time
                        values.update(base_item)
                        yield values


class PageInsightsStream(InsightsStream):
    """Base class for Page Insights streams"""
    parent_stream_type = PagesStream
    path = "/{page_id}/insights"

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        params = super().get_url_params(context, next_page_token)
        # params["period"] = "day"  # TODO: might need separate day and lifetime base classes
        # TODO: pagination with since and until in smaller batches
        if self.get_starting_timestamp(context):
            # extra 2 day look back in case metrics are updated late
            params["since"] = pendulum.instance(
                self.get_starting_timestamp(context)
            ).subtract(days=2).to_date_string()
        else:
            # `since` date isn't included in the range so subtract 1 day
            params["since"] = pendulum.parse(self.config["start_date"]).subtract(days=1).to_date_string()
        params["access_token"] = self.page_access_tokens[context["page_id"]]
        params["metric"] = ",".join(self.metrics)
        return params


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
        "page_video_view_time",
    ]


class PageVideoViews2InsightsStream(PageInsightsStream):
    """https://developers.facebook.com/docs/graph-api/reference/insights#videoviews"""
    name = "page_video_views_2_insights"
    metrics = [
        "page_video_complete_views_30s",
        "page_video_complete_views_30s_paid",
        "page_video_complete_views_30s_organic",
        "page_video_complete_views_30s_autoplayed",
        "page_video_complete_views_30s_click_to_play",
        "page_video_complete_views_30s_unique",
        "page_video_complete_views_30s_repeat_views",
        "page_video_views_10s",
        "page_video_views_10s_paid",
        "page_video_views_10s_organic",
        "page_video_views_10s_autoplayed",
        "page_video_views_10s_click_to_play",
        "page_video_views_10s_unique",
        "page_video_views_10s_repeat",
    ]


class PageVideoAdBreaksInsightsStream(PageInsightsStream):
    """https://developers.facebook.com/docs/graph-api/reference/insights#video-ad-breaks"""
    name = "page_video_ad_breaks_insights"
    metrics = [
        "page_daily_video_ad_break_ad_impressions_by_crosspost_status",
        "page_daily_video_ad_break_cpm_by_crosspost_status",
        "page_daily_video_ad_break_earnings_by_crosspost_status",
    ]


class PostInsightsStream(InsightsStream):
    """https://developers.facebook.com/docs/graph-api/reference/insights#page-video-posts"""
    name = "post_insights"
    parent_stream_type = AllPostsStream
    path = "/{post_id}/insights"
    state_partitioning_keys = ["page_id"]  # too many posts to store state for each one
    metrics = [  # TODO: allow selection from config, make this dynamic
        "post_engaged_users",  # day, day_28, week, lifetime, month
        "post_negative_feedback",
        "post_negative_feedback_unique",
        "post_negative_feedback_by_type",
        "post_negative_feedback_by_type_unique",
        "post_engaged_fan",
        "post_clicks",
        "post_clicks_unique",
        "post_clicks_by_type",
        "post_clicks_by_type_unique",
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
        "post_reactions_by_type_total",
        "post_video_complete_views_30s_autoplayed",
        "post_video_complete_views_30s_clicked_to_play",
        "post_video_complete_views_30s_organic",
        "post_video_complete_views_30s_paid",
        "post_video_complete_views_30s_unique",
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
        "post_video_view_time_by_age_bucket_and_gender",
        "post_video_view_time_by_region_id",
        "post_video_views_by_distribution_type",
        "post_video_view_time_by_distribution_type",
        "post_video_view_time_by_country_id",
        "post_video_ad_break_ad_impressions",
        "post_video_ad_break_earnings",
        "post_video_ad_break_ad_cpm",
    ]

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        params = super().get_url_params(context, next_page_token)
        # params["period"] = "day"  # TODO: might need separate day and lifetime base classes
        # if no state is found, get all data since the post was published
        # this is guaranteed to be in the last X months, where X is the configured `insights_lookback_months`
        params["since"] = pendulum.instance(
            self.get_starting_timestamp(context) or context["created_time"]
        ).subtract(days=1).to_date_string()
        params["access_token"] = self.page_access_tokens[context["page_id"]]
        params["metric"] = ",".join(self.metrics)
        return params


class RecentPostInsightsStream(FacebookPagesStream):
    """
    Post insights fetched from the /published_posts endpoint.
    """
    name = "recent_post_insights"
    parent_stream_type = PagesStream
    path = "/{page_id}/published_posts"
    primary_keys = ["id"]
    replication_key = None
    schema_filepath = SCHEMAS_DIR / "post_insights.json"
    records_jsonpath = "$.data[*]"
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
        # there last 3 metrics throw a permissions error
        # seems like we can access monetization from /insights but not /published_posts
        # "post_video_ad_break_ad_impressions",
        # "post_video_ad_break_earnings",
        # "post_video_ad_break_ad_cpm",
    ]

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        params = super().get_url_params(context, next_page_token)
        params["access_token"] = self.page_access_tokens[context["page_id"]]
        params["fields"] = f"id,created_time,insights.metric({','.join(self.metrics)})"
        return params

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        resp_json = response.json()
        if "data" not in resp_json:
            self.logger.warning(f"No data found: {resp_json}")
            return
        for row in resp_json["data"]:
            for insights in row["insights"]["data"]:
                base_item = {
                    "post_id": row["id"],
                    "post_created_time": row["created_time"],
                    "name": insights["name"],
                    "period": insights["period"],
                    "title": insights["title"],
                    "description": insights["description"],
                    "id": insights["id"],
                }
                if "values" in insights:
                    for values in insights["values"]:
                        if isinstance(values["value"], dict):
                            for key, value in values["value"].items():
                                item = {
                                    "context": key,
                                    "value": value,
                                }
                                item.update(base_item)
                                yield item
                        else:
                            values.update(base_item)
                            if "end_time" in values:
                                values["end_time"] = pendulum.parse(values["end_time"]).to_datetime_string()
                            yield values


# deprecated in favor of PagePostsInsightsStream
# class PagePostEngagementInsightsStream(PostInsightsStream):
#     """https://developers.facebook.com/docs/graph-api/reference/insights#page-post-engagement"""
#     name = "page_post_engagement_insights"
#     metrics = [
#         "post_engaged_users",
#         "post_negative_feedback",
#         "post_negative_feedback_unique",
#         "post_negative_feedback_by_type",
#         "post_negative_feedback_by_type_unique",
#         "post_engaged_fan",
#         "post_clicks",
#         "post_clicks_unique",
#         "post_clicks_by_type",
#         "post_clicks_by_type_unique",
#     ]


# deprecated in favor of PagePostsInsightsStream
# class PagePostImpressionsInsightsStream(PostInsightsStream):
#     """https://developers.facebook.com/docs/graph-api/reference/insights#page-post-impressions"""
#     name = "page_post_impressions_insights"
#     metrics = [
#         "post_impressions",
#         "post_impressions_unique",
#         "post_impressions_paid",
#         "post_impressions_paid_unique",
#         "post_impressions_fan",
#         "post_impressions_fan_unique",
#         "post_impressions_fan_paid",
#         "post_impressions_fan_paid_unique",
#         "post_impressions_organic",
#         "post_impressions_organic_unique",
#         "post_impressions_viral",
#         "post_impressions_viral_unique",
#         "post_impressions_nonviral",
#         "post_impressions_nonviral_unique",
#         "post_impressions_by_story_type",
#         "post_impressions_by_story_type_unique",
#     ]


# deprecated in favor of PagePostsInsightsStream
# class PostVideoAdBreaksInsightsStream(PostInsightsStream):
#     """https://developers.facebook.com/docs/graph-api/reference/insights#video-ad-breaks"""
#     name = "post_video_ad_breaks_insights"
#     metrics = [
#         "post_video_ad_break_ad_impressions",
#         "post_video_ad_break_earnings",
#         "post_video_ad_break_ad_cpm",
#     ]
