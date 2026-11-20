"""Real-time connected-vehicle telemetry Lakehouse.

Shared building blocks used by the generator, the streaming jobs and the tests:

- ``schema``    the event contract (one source of truth for Python and Spark)
- ``quality``   data-quality rules, each defined once with a Python predicate
                and an equivalent Spark SQL expression
- ``generator`` a synthetic fleet that emits realistic events, including
                late, duplicate and malformed records on purpose
"""

__version__ = "0.1.0"
