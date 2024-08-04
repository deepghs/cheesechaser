"""
This module provides utilities for making HTTP requests with enhanced functionality.

It includes classes and functions for:

- Creating custom HTTP adapters with default timeouts
- Generating random user agents
- Creating and configuring requests sessions with retries and timeouts
- Making HTTP requests with automatic retries and error handling

The module supports both the `requests` and `httpx` libraries for making HTTP requests.
"""

import time
import warnings
from functools import lru_cache
from typing import Optional, Dict, Union

import httpx
import requests
from random_user_agent.params import SoftwareName, OperatingSystem
from random_user_agent.user_agent import UserAgent
from requests.adapters import HTTPAdapter, Retry

DEFAULT_TIMEOUT = 10  # seconds


class TimeoutHTTPAdapter(HTTPAdapter):
    """
    Custom HTTP adapter that sets a default timeout for requests.

    Inherits from `HTTPAdapter`.

    Usage:
    - Create an instance of `TimeoutHTTPAdapter` and pass it to a `requests.Session` object's `mount` method.

    Example:
        >>> session = requests.Session()
        >>> adapter = TimeoutHTTPAdapter(timeout=10)
        >>> session.mount('http://', adapter)
        >>> session.mount('https://', adapter)

    :param timeout: The default timeout value in seconds. (default: 10)
    :type timeout: int
    """

    def __init__(self, *args, **kwargs):
        self.timeout = DEFAULT_TIMEOUT
        if "timeout" in kwargs:
            self.timeout = kwargs["timeout"]
            del kwargs["timeout"]
        super().__init__(*args, **kwargs)

    def send(self, request, **kwargs):
        """
        Sends a request with the provided timeout value.

        :param request: The request to send.
        :type request: PreparedRequest
        :param kwargs: Additional keyword arguments.
        :type kwargs: dict
        :returns: The response from the request.
        :rtype: Response
        """
        timeout = kwargs.get("timeout")
        if timeout is None:
            kwargs["timeout"] = self.timeout
        return super().send(request, **kwargs)


def get_requests_session(max_retries: int = 5, timeout: int = DEFAULT_TIMEOUT,
                         headers: Optional[Dict[str, str]] = None,
                         session: Optional[httpx.Client] = None, use_httpx: bool = False) \
        -> Union[httpx.Client, requests.Session]:
    """
    Creates and configures a requests or httpx session with retries, timeouts, and custom headers.

    This function can create a new session or modify an existing one. It supports both the `requests`
    and `httpx` libraries.

    :param max_retries: Maximum number of retries for failed requests. (default: 5)
    :type max_retries: int
    :param timeout: Timeout value in seconds for requests. (default: DEFAULT_TIMEOUT)
    :type timeout: int
    :param headers: Additional headers to add to the session. (default: None)
    :type headers: Optional[Dict[str, str]]
    :param session: An existing session to modify. If None, a new session is created. (default: None)
    :type session: Optional[httpx.Client]
    :param use_httpx: Whether to use httpx instead of requests. (default: False)
    :type use_httpx: bool
    :return: A configured requests.Session or httpx.Client object.
    :rtype: Union[httpx.Client, requests.Session]
    """
    if not session:
        if use_httpx:
            session = httpx.Client(http2=True, timeout=timeout, follow_redirects=True)
        else:
            session = requests.session()
    if isinstance(session, requests.Session):
        retries = Retry(
            total=max_retries, backoff_factor=1,
            status_forcelist=[408, 413, 429, 500, 501, 502, 503, 504, 505, 506, 507, 509, 510, 511],
            allowed_methods=["HEAD", "GET", "POST", "PUT", "DELETE", "OPTIONS", "TRACE"],
        )
        adapter = TimeoutHTTPAdapter(max_retries=retries, timeout=timeout, pool_connections=32, pool_maxsize=32)
        session.mount('http://', adapter)
        session.mount('https://', adapter)
    session.headers.update({
        "User-Agent": get_random_ua(),
        **(headers or {}),
    })

    return session


RETRY_ALLOWED_METHODS = ["HEAD", "GET", "POST", "PUT", "DELETE", "OPTIONS", "TRACE"]
RETRY_STATUS_FORCELIST = [413, 429, 500, 501, 502, 503, 504, 505, 506, 507, 509, 510, 511]


def _should_retry(response: httpx.Response) -> bool:
    """
    Determines if a request should be retried based on its method and status code.

    :param response: The response object to check.
    :type response: httpx.Response
    :return: True if the request should be retried, False otherwise.
    :rtype: bool
    """
    return response.request.method in RETRY_ALLOWED_METHODS and \
        response.status_code in RETRY_STATUS_FORCELIST


def srequest(session: httpx.Client, method, url, *, max_retries: int = 5,
             backoff_factor: float = 1.0, raise_for_status: bool = True, **kwargs) -> httpx.Response:
    """
    Sends an HTTP request with automatic retries and error handling.

    This function uses exponential backoff for retries and can raise exceptions for HTTP errors.

    :param session: The httpx.Client session to use for the request.
    :type session: httpx.Client
    :param method: The HTTP method to use (e.g., 'GET', 'POST').
    :type method: str
    :param url: The URL to send the request to.
    :type url: str
    :param max_retries: Maximum number of retries for failed requests. (default: 5)
    :type max_retries: int
    :param backoff_factor: Factor to calculate the exponential backoff time between retries. (default: 1.0)
    :type backoff_factor: float
    :param raise_for_status: Whether to raise an exception for HTTP errors. (default: True)
    :type raise_for_status: bool
    :param kwargs: Additional keyword arguments to pass to the request method.
    :return: The response object from the successful request.
    :rtype: httpx.Response
    :raises: Various exceptions related to HTTP errors and request failures.
    """
    resp = None
    for i in range(max_retries):
        sleep_time = backoff_factor * (2 ** i)
        try:
            resp = session.request(method, url, **kwargs)
            if raise_for_status:
                resp.raise_for_status()
        except (httpx.TooManyRedirects,):
            raise
        except (httpx.HTTPStatusError, requests.exceptions.HTTPError) as err:
            if _should_retry(err.response):
                warnings.warn(f'Requests {err.response.status_code} ({i + 1}/{max_retries}), '
                              f'sleep for {sleep_time!r}s ...')
                time.sleep(sleep_time)
            else:
                raise
        except (httpx.HTTPError, requests.exceptions.RequestException) as err:
            warnings.warn(f'Requests error ({i + 1}/{max_retries}): {err!r}, '
                          f'sleep for {sleep_time!r}s ...')
            time.sleep(sleep_time)
        else:
            break

    assert resp is not None, f'Request failed for {max_retries} time(s) - {method} {url!r}.'
    if raise_for_status:
        resp.raise_for_status()

    return resp


@lru_cache()
def _ua_pool():
    """
    Creates and caches a UserAgent object for generating random user agents.

    This function is cached to avoid recreating the UserAgent object on every call.

    :return: A UserAgent object configured with specific software names and operating systems.
    :rtype: UserAgent
    """
    software_names = [SoftwareName.CHROME.value, SoftwareName.FIREFOX.value, SoftwareName.EDGE.value]
    operating_systems = [OperatingSystem.WINDOWS.value, OperatingSystem.MACOS.value]

    user_agent_rotator = UserAgent(software_names=software_names, operating_systems=operating_systems, limit=1000)
    return user_agent_rotator


def get_random_ua():
    """
    Generates a random user agent string.

    This function uses the cached UserAgent object to generate a random user agent.

    :return: A random user agent string.
    :rtype: str
    """
    return _ua_pool().get_random_user_agent()
