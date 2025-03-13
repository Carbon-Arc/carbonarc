from requests.auth import AuthBase


class TokenAuth(AuthBase):
    """
    Class for authenticating requests by user supplied token.
    """

    def __init__(self, token: str):
        assert token, "Token must be a non-empty string."
        self.auth_token = token

    def __call__(self, r):
        """
        Override the default __call__ method for the AuthBase base class

        More more info, see:
        https://docs.python-requests.org/en/master/user/advanced/
        """
        auth_token = self.auth_token
        r.headers["Authorization"] = "Bearer " + auth_token
        return r
