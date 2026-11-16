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
    """Raised when API rate limit is exceeded."""
    pass

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

# -- Train (client.train, preview) --------------------------------------------
#
# power-api answers a refused Train request with ``{"error_code", "detail"}``.
# ``carbonarc.train`` maps each code to one of these (its ``_REFUSALS`` table).
# Each also subclasses the generic exception for its HTTP status, so code that
# already catches ``NotFoundError`` or ``ForbiddenError`` keeps working.


class TrainError(CarbonArcException):
    """Base for every refusal from the Train API. ``error_code`` is power-api's
    code (``TRAIN_RIGHTS``, ``TRAIN_DATA``...), or None when it sent none."""

    def __init__(self, message, status_code=None, response=None, error_code=None):
        super().__init__(message, status_code=status_code, response=response)
        self.error_code = error_code


class TrainNotEnabled(TrainError, NotFoundError):
    """Train isn't turned on for this account (HTTP 404). Raised by calls that
    name no framework, run or model, where a 404 can only mean that."""


class TrainNotFound(TrainError, NotFoundError):
    """No framework, run or model with that id for this account (HTTP 404).
    power-api gives the same answer when Train isn't turned on for the
    account, so the message says both."""


class NoTrainingGrant(TrainError, ForbiddenError):
    """The data's training rights don't allow this (HTTP 403): no training
    grant, or one revoked since the run was launched."""


class NotEnoughData(TrainError, ValidationError):
    """The framework can't be trained as asked (HTTP 400): too few series, a
    window too short, or a horizon too long. The message says which."""


class TrainRunExists(TrainError):
    """The account already has a run of this config queued, running or done
    (HTTP 409). ``existing_run_id`` is that run's id."""

    def __init__(self, message, existing_run_id=None, **kwargs):
        super().__init__(message, **kwargs)
        self.existing_run_id = existing_run_id


class TrainBusy(TrainError):
    """Every training machine is busy (HTTP 503). ``retry_after`` is the
    seconds power-api suggests waiting, or None when it gave none."""

    def __init__(self, message, retry_after=None, **kwargs):
        super().__init__(message, **kwargs)
        self.retry_after = retry_after


class TrainPlatformError(TrainError):
    """Something failed on Carbon Arc's side: usually HTTP 500, or a 403 when
    the service that checks training grants can't be reached. Not about your
    data or rights: try again shortly."""
