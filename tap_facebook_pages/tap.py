"""FacebookPages tap class."""

import json
from typing import List

import requests
from singer_sdk import Stream, Tap
from singer_sdk import typing as th  # JSON schema typing helpers

# TODO: Import your custom stream types here:
from tap_facebook_pages.streams import (
    AllPostsStream,
    PageEngagementInsightsStream,
    PageImpressionsInsightsStream,
    PagePostsInsightsStream,
    PagesStream,
    PageVideoAdBreaksInsightsStream,
    PageVideoViews2InsightsStream,
    PageVideoViewsInsightsStream,
    PostInsightsStream,
    PostsStream,
    RecentPostInsightsStream,
    VideoStream,
)

# TODO: post_ids in historical insights are null
STREAM_TYPES = [
    # AllPostsStream,
    PageEngagementInsightsStream,
    PageImpressionsInsightsStream,
    PagePostsInsightsStream,
    PagesStream,
    # PageVideoAdBreaksInsightsStream,
    PageVideoViewsInsightsStream,
    PageVideoViews2InsightsStream,
    # PostInsightsStream,
    # PostsStream,
    # RecentPostInsightsStream,
    # VideoStream,
]

# TODO: define these just once
GRAPH_API_VERSION = "15.0"
BASE_URL = f"https://graph.facebook.com/v{GRAPH_API_VERSION}"

session = requests.Session()


class TapFacebookPages(Tap):
    """FacebookPages tap class."""

    name = "tap-facebook-pages"

    # TODO: Configurable threshold for post age cutoff when fetching insights
    config_jsonschema = th.PropertiesList(
        th.Property(
            "user_token",
            th.StringType,
            required=True,
            secret=True,  # Flag config as protected.
            description="Long-lived user access token with access to all pages.",
        ),
        # th.Property(
        #     "page_ids",
        #     th.ArrayType(th.StringType),
        #     required=True,
        #     description="Page IDs of Facebook pages for which to fetch data."
        # ),
        th.Property(
            "pages",
            th.ArrayType(
                th.ObjectType(
                    th.Property("id", th.StringType, required=True),
                    th.Property(
                        "access_token", th.StringType, required=True, secret=True
                    ),
                )
            ),
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
            description="Start date for fetching historical data.",
            default="2022-10-01T00:00:00Z",
        ),
        th.Property(
            "insights_lookback_months",
            th.IntegerType,
            description="The lookback period for fetching insights data. Defaults to 12 months, meaning that we fetch "
            "insights only for the last 12 months and for posts published within the last 12 months.",
            default=24,
        ),
    ).to_dict()

    def exchange_token(self, page_id: str):
        url = f"{BASE_URL}/{page_id}"
        data = {
            "fields": "access_token,name",
            "access_token": self.config["user_token"],
        }

        self.logger.info("Exchanging access token for page with id=" + page_id)
        response = session.get(url=url, params=data)
        response_data = json.loads(response.text)
        if response.status_code != 200:
            error_message = (
                "Failed exchanging token: " + response_data["error"]["message"]
            )
            self.logger.error(error_message)
            raise RuntimeError(error_message)
        self.logger.info(
            "Successfully exchanged access token for page with id=" + page_id
        )
        return response_data["access_token"]

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        # This feels like the cleanest way to ensure we only exchange tokens once and that
        # all streams have access to the page tokens. Overriding __init__() in the base
        # stream class could work but feels riskier.
        # TODO: do this just for the base class?
        for stream_class in STREAM_TYPES:
            stream_class.page_access_tokens = {
                page["id"]: page["access_token"] for page in self.config["pages"]
            }
        return [stream_class(tap=self) for stream_class in STREAM_TYPES]


if __name__ == "__main__":
    TapFacebookPages.cli()
