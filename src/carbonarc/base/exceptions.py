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


class NotFoundError(CarbonArcException):
    """Raised when a resource is not found."""
    pass


class ValidationError(CarbonArcException):
    """Raised when request validation fails."""
    pass


class RateLimitError(CarbonArcException):
    """Raised when API rate limit is exceeded."""
    pass

class InvalidConfigurationError(CarbonArcException):
    """Raised when the configuration is invalid."""
    pass