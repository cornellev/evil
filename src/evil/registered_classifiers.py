"""The single place every registered classifier is listed. Both bulk
ingestion (parser.py, scripts/ingest_recording.py, scripts/reclassify.py) and the
live compiler (scripts/run_live_compiler.py) import this rather than keeping their
own copies. `laps` depends_on "segments" (it counts the turns driven in a lap), so
the two must always be registered together, in exactly one place to stay in sync.
"""

from __future__ import annotations

from evil.classifiers.base import Classifier
from evil.classifiers.laps import LapsClassifier
from evil.classifiers.segments import SegmentsClassifier

ALL_CLASSIFIERS: list[Classifier] = [SegmentsClassifier(), LapsClassifier()]
