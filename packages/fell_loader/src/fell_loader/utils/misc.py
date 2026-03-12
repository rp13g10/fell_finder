"""General-purpose utility functions"""

import logging
import os
from functools import lru_cache

logger = logging.getLogger(__name__)


@lru_cache
def get_env_var(
    env_var: str, cast: type | None = None, default: str | None = None
) -> str:
    """Fetch the requested environment variable, and if required cast it to the
    specified type. By default all environment variables will be returned as
    strings.

    Args:
        env_var: The environment variable to fetch
        cast: The type (e.g. `int`) which the environment variable should be
            cast to before returning. Set to None to return directly as a
            string. Defaults to None.
        default: The default value which should be returned if the requested
            variable cannot be found. This should be provided as a string,
            the provided `cast` will be applied in cases where the default
            value must be returned. If set to None, then attempting to
            access an unset variable will raise an Exception. Defaults to None.

    Raises:
        KeyError: If the requested environment variable has not been set, and
            no default value is specified, an exception will be raised.

    Returns:
        The value of the requested environment variable
    """
    try:
        val = os.environ[env_var]
    except KeyError:
        if default is not None:
            logger.warning(
                (
                    "Environment variable '%s', not set. Falling back on "
                    "default value %s"
                ),
                env_var,
                default,
            )
            val = default
        else:
            raise KeyError(
                f"Unable to retrieve environment variable {env_var}, and no "
                "default value has been set"
            )

    if cast is not None:
        val = cast(val)

    return val
