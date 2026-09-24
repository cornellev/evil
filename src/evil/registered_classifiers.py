"""The single place every registered classifier is listed. Both bulk
ingestion (scripts/ingest_recording.py) and the live compiler
(scripts/run_live_compiler.py) import this rather than keeping their own
copies -- laps and straights both depends_on "turns", so all three must
always be registered together, in exactly one place to stay in sync.
"""

from __future__ import annotations

from evil.classifiers.base import Classifier
from evil.classifiers.laps import LapsClassifier
from evil.classifiers.straights import StraightsClassifier
from evil.classifiers.turns import TurnsClassifier

ALL_CLASSIFIERS: list[Classifier] = [TurnsClassifier(), LapsClassifier(), StraightsClassifier()]
