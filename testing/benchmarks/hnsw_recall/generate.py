#!/usr/bin/env python3
"""Generate the clustered and uniform corpus used by the HNSW recall benchmark."""

import argparse
from pathlib import Path

import numpy as np


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)

    dimensions = 128
    rng = np.random.default_rng(1)
    unit = lambda value: value / np.linalg.norm(value, axis=-1, keepdims=True)
    near = lambda center, scale, count=None: unit(
        center
        + scale
        * unit(
            rng.standard_normal(
                (count, dimensions) if count else center.shape
            )
        )
    )

    clustered = []
    clustered_queries = []
    for _ in range(40):
        topic = unit(rng.standard_normal(dimensions))
        for document_index in range(50):
            document = near(topic, 0.7)
            clustered.append(near(document, 0.42, 50))
            if document_index % 10 == 0:
                clustered_queries.append(near(document, 0.6))

    clustered = np.vstack(clustered).astype("float32")
    clustered_queries = np.vstack(clustered_queries).astype("float32")
    uniform = unit(
        rng.standard_normal((len(clustered), dimensions))
    ).astype("float32")
    uniform_queries = near(
        uniform[rng.integers(0, len(uniform), len(clustered_queries))],
        0.6,
    ).astype("float32")

    for name, values in (
        ("clustered", clustered),
        ("clustered_q", clustered_queries),
        ("uniform", uniform),
        ("uniform_q", uniform_queries),
    ):
        values.tofile(args.output / f"{name}.f32")
    print("vectors", clustered.shape, "queries", clustered_queries.shape)


if __name__ == "__main__":
    main()
