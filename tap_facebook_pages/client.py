"""REST client handling, including FacebookPagesStream base class."""

import re
import urllib.parse
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Union

import requests
from memoization import cached
from singer_sdk.authenticators import APIKeyAuthenticator
from singer_sdk.exceptions import FatalAPIError, RetriableAPIError
from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.streams import RESTStream

SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")
GRAPH_API_VERSION = 15.0


class FacebookPagesStream(RESTStream):
    """FacebookPages stream class."""

    # TODO: Set the API's base URL here:
    url_base = f"https://graph.facebook.com/v{GRAPH_API_VERSION}"

    # OR use a dynamic url_base:
    # @property
    # def url_base(self) -> str:
    #     """Return the API URL root, configurable via tap settings."""
    #     return self.config["api_url"]

    records_jsonpath = "$[*]"  # Or override `parse_response`.
    next_page_token_jsonpath = (
        "$.paging.cursors.after"  # Or override `get_next_page_token`.
    )
    page_access_tokens: Dict[str, str] = None

    @property
    def partitions(self) -> List[Dict[str, str]]:
        return [{"page_id": page["id"]} for page in self.config["pages"]]

    # @property
    # def authenticator(self) -> APIKeyAuthenticator:
    #     """Return a new authenticator object."""
    #     return APIKeyAuthenticator.create_for_stream(
    #         self,
    #         key="access_token",
    #         value=self.config.get("api_key"),
    #         location="params"
    #     )

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed."""
        headers = {}
        if "user_agent" in self.config:
            headers["User-Agent"] = self.config.get("user_agent")
        # If not using an authenticator, you may also provide inline auth headers:
        # headers["Private-Token"] = self.config.get("auth_token")
        return headers

    def backoff_max_tries(self) -> int:
        """
        Increase from default of 5 to 10 since FB often throws 500s that need more attempts.
        Paired with the default exponential backoff strategy, this should help!
        """
        return 10

    def get_next_page_token(
        self, response: requests.Response, previous_token: Optional[Any]
    ) -> Optional[Any]:
        """Return a token for identifying next page or None if no more pages."""
        # TODO: If pagination is required, return a token which can be used to get the
        #       next page. If this is the final page, return "None" to end the
        #       pagination loop.
        if self.next_page_token_jsonpath:
            all_matches = extract_jsonpath(
                self.next_page_token_jsonpath, response.json()
            )
            first_match = next(iter(all_matches), None)
            next_page_token = first_match
        else:
            next_page_token = response.headers.get("X-Next-Page", None)

        # TODO: FB's Graph API seems to return identical token?
        # if next_page_token != previous_token:
        return next_page_token

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        params: dict = {"access_token": self.config["user_token"]}
        if next_page_token:
            params["after"] = next_page_token
        if self.replication_key:
            params["sort"] = "asc"
            params["order_by"] = self.replication_key
        return params

    def prepare_request(
        self, context: Optional[dict], next_page_token: Optional[Any] = None
    ) -> requests.PreparedRequest:
        req = super().prepare_request(context, next_page_token)
        # self.logger.info(urllib.parse.unquote(req.url))
        self.logger.info(
            re.sub(
                "access_token=[a-zA-Z0-9]+&",
                "access_token=*****&",
                urllib.parse.unquote(req.url),
            )
        )
        return req

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the response and return an iterator of result records."""
        # TODO: Parse response body and return a set of records.
        yield from extract_jsonpath(self.records_jsonpath, input=response.json())

    def validate_response(self, response: requests.Response) -> None:
        # TODO: Handle 100 response code
        # TODO: Handle "reduce data you're asking for" for videos stream
        if (400 <= response.status_code < 500) or (response.status_code == 100):
            msg = (
                f"{response.status_code} Client Error: "
                f"{response.reason} for path: {self.path}: "
                f"{response.json().get('error', {}).get('message')}"
            )
            # If a video/post is not found when attempting to fetch insights
            # this should not stop the entire sync. Log and move on!
            not_exists_pattern = re.compile(
                "^.*Object with ID '[0-9]+.*' does not exist.*$"
            )
            # Unclear why this error is thrown but the assumption
            # is that the video/post no longer exists. Skip these.
            unsupported_pattern = re.compile(
                "^.*Unsupported request - method type: get.*$"
            )

            # This error can be thrown on videos/posts that don't belong to us
            permissions_error = re.compile(
                "^.*This endpoint requires the '.*' permission.*$"
            )

            error_message = response.json().get("error", {}).get("message")

            if response.status_code in (100, 400) and (
                not_exists_pattern.match(error_message)
                or unsupported_pattern.match(error_message)
                or permissions_error.match(error_message)
            ):
                self.logger.warning(f"Skipping record because object not found: {msg}")
                return
            # The Graph API occasionally complains that we need to use a page access token
            # even though we should already be using one. A retry appears to resolve this.
            if (
                response.status_code == 400
                and error_message
                == "(#190) This method must be called with a Page Access Token"
            ):
                raise RetriableAPIError(msg)
            monetization_access_pattern = re.compile(
                "^.*Monetization metrics are only visible for Page admins.*$"
            )
            if response.status_code == 403 and monetization_access_pattern.match(
                error_message
            ):
                self.logger.warning(f"Skipping record because: {msg}")
                return
            # FB will occasionally throw a 500 with this vague message - might as well retry :shrug:
            # if (
            #     response.status_code == 500
            #     and error_message ==
            #     "An unknown error occurred"
            # ):
            #     raise RetriableAPIError(msg)
            raise FatalAPIError(msg)
        super().validate_response(response)
