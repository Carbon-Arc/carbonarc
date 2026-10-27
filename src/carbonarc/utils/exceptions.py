class CarbonArcException(Exception):
    """Base exception for all errors."""

    def __init__(self, message, status_code=None, response=None):
        self.message = message
        self.status_code = status_code
        self.response = response
        super().__init__(self.message)


class AuthenticationError(CarbonArcException):
    """Raised when authentication fails."""
    pass


class ForbiddenError(CarbonArcException):
    """Raised when the caller is authenticated but lacks the role/plan
    needed for the operation (HTTP 403). Distinct from
    :class:`AuthenticationError` (the token itself is invalid)."""
    pass


class NotFoundError(CarbonArcException):
    """Raised when a resource is not found."""
    pass


class ValidationError(CarbonArcException):
    """Raised when request validation fails."""
    pass


class RateLimitError(CarbonArcException):
    """Raised when API rate limit is exceeded (HTTP 429).

    ``retry_after`` is the number of seconds the API asked the caller to wait,
    taken from the ``Retry-After`` response header. It is ``None`` when the
    header is absent or carries an HTTP-date rather than a number of seconds.
    """

    def __init__(self, message, status_code=None, response=None, retry_after=None):
        super().__init__(message, status_code=status_code, response=response)
        self.retry_after = retry_after

class InvalidConfigurationError(CarbonArcException):
    """Raised when the configuration is invalid."""
    pass


class QueryJobFailedError(CarbonArcException):
    """Raised when a polled query job (framework price/filters/buy/data
    behind the poll=true job-queue contract) reaches state="failed"."""
    pass


class QueryJobCancelledError(CarbonArcException):
    """Raised when a polled query job reaches state="cancelled" -- either
    this caller's own ``cancel_query_job()`` or another caller/thread with
    the same job_id."""
    pass


class QueryJobTimeoutError(CarbonArcException):
    """Raised when ``poll_job_to_completion``'s optional ``max_wait_seconds``
    elapses before the job reaches a terminal state. Client-side defense in
    depth only: the server has its own bounds on how long a job can sit
    queued or running before it self-evicts to state="failed", so this
    should be rare in practice -- it exists for the case where the server
    never responds with a terminal state at all (a future regression in
    that server-side eviction, or a job stuck in an unanticipated way)."""
    pass