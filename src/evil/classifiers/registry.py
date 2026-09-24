"""Orders registered classifiers so one that depends on another classifier's
output (e.g. laps depending on turns) always runs after it within a tick."""

from __future__ import annotations

from evil.classifiers.base import Classifier


def topological_order(classifiers: list[Classifier]) -> list[Classifier]:
    by_name = {c.spec.name: c for c in classifiers}
    visited: dict[str, int] = {}  # 0 = in progress, 1 = done
    ordered: list[Classifier] = []

    def visit(c: Classifier, stack: tuple[str, ...]) -> None:
        state = visited.get(c.spec.name)
        if state == 1:
            return
        if state == 0:
            cycle = " -> ".join(stack + (c.spec.name,))
            raise ValueError(f"cycle in classifier dependencies: {cycle}")

        visited[c.spec.name] = 0
        for dep in c.spec.depends_on:
            dep_classifier = by_name.get(dep)
            if dep_classifier is not None:
                visit(dep_classifier, stack + (c.spec.name,))
        visited[c.spec.name] = 1
        ordered.append(c)

    for classifier in classifiers:
        visit(classifier, ())

    return ordered
