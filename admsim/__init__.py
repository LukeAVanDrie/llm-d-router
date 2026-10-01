"""A discrete-event simulator of a vLLM-shaped engine pool behind an admission-controlling router."""

from .experiment import batch, run  # noqa: F401
from .simulation import simulate  # noqa: F401
