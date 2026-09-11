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