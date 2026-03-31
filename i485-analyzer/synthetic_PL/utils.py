"""Shared utility functions for synthetic data generators."""
import numpy as np
import pandas as pd


def pick(rng, options, size, weights=None):
    """Randomly sample *size* items from *options* with optional weights."""
    opts = np.asarray(options)
    if weights is not None:
        w = np.asarray(weights, dtype=float)
        w = w / w.sum()  # normalise
    else:
        w = None
    return rng.choice(opts, size=int(size), replace=True, p=w)


def gen_dates(rng, n, start, end):
    """Generate *n* random dates uniformly between *start* and *end* (strings)."""
    s = np.datetime64(start)
    e = np.datetime64(end)
    span = (e - s).astype(int)
    offsets = rng.integers(0, max(span, 1), size=n)
    return pd.Series(s + offsets.astype("timedelta64[D]")).dt.strftime("%Y-%m-%d").values


def gen_timestamps(rng, n, start="2020-01-01", end="2026-02-28"):
    """Generate *n* random timestamps (date + time)."""
    s = np.datetime64(start)
    e = np.datetime64(end)
    span = int((e - s) / np.timedelta64(1, "s"))
    offsets = rng.integers(0, max(span, 1), size=n)
    base = pd.Timestamp(start)
    return pd.Series([base + pd.Timedelta(seconds=int(o)) for o in offsets]).dt.strftime(
        "%Y-%m-%d %H:%M:%S"
    ).values
