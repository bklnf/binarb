class BinanceError(RuntimeError):
    def __init__(self, message, *, code=None, endpoint=None, ambiguous=False):
        super().__init__(str(message))
        self.code = code
        self.endpoint = endpoint
        self.ambiguous = ambiguous


class AuthenticationError(BinanceError):
    pass


class RateLimitError(BinanceError):
    def __init__(self, message, *, retry_after_s=None, **kwargs):
        super().__init__(message, **kwargs)
        self.retry_after_s = retry_after_s


class AmbiguousOrderError(BinanceError):
    pass


class RecoveryRequired(RuntimeError):
    pass
