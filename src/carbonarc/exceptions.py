class CarbonArcException(Exception):
    """Base class for Carbon Arc Exceptions"""

    pass


class AuthenticationException(CarbonArcException):
    """Valid Token required Exception"""

    pass
