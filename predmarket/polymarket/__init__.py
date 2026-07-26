"""Read-only Polymarket public-data adapters."""


class AdapterError(RuntimeError):
    """Base class for an adapter failure."""


class AdapterTransportError(AdapterError):
    """The HTTP exchange did not complete."""


class AdapterHTTPError(AdapterError):
    """The remote endpoint returned a non-success status."""


class AdapterPayloadError(AdapterError):
    """The response was not valid JSON in the documented shape."""


class AdapterInvariantError(AdapterError):
    """The response violated a cross-record or market invariant."""


class AdapterSecurityError(AdapterInvariantError, ValueError):
    """A public-data client was configured with credentials."""


__all__ = [
    "AdapterError",
    "AdapterTransportError",
    "AdapterHTTPError",
    "AdapterPayloadError",
    "AdapterInvariantError",
    "AdapterSecurityError",
]
