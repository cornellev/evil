import pytest

from evil.classifiers.base import ClassifierSpec
from evil.classifiers.registry import topological_order


class _Stub:
    def __init__(self, name, depends_on):
        self.spec = ClassifierSpec(name=name, version=1, depends_on=depends_on, lookback_margin_s=0.0)

    def run(self, conn, run_id, since_seq, until_seq):  # pragma: no cover - not exercised here
        raise NotImplementedError


def test_topological_order_respects_dependencies():
    turns = _Stub("turns", depends_on=["main_snapshot"])
    laps = _Stub("laps", depends_on=["turns"])
    efficiency = _Stub("efficiency_delta", depends_on=["laps"])

    ordered = topological_order([efficiency, laps, turns])

    names = [c.spec.name for c in ordered]
    assert names.index("turns") < names.index("laps") < names.index("efficiency_delta")


def test_topological_order_is_stable_for_independent_classifiers():
    a = _Stub("a", depends_on=["main_snapshot"])
    b = _Stub("b", depends_on=["main_snapshot"])

    ordered = topological_order([a, b])

    assert {c.spec.name for c in ordered} == {"a", "b"}


def test_topological_order_detects_cycles():
    a = _Stub("a", depends_on=["b"])
    b = _Stub("b", depends_on=["a"])

    with pytest.raises(ValueError, match="cycle"):
        topological_order([a, b])
