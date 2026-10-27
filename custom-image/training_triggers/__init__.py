"""Training triggers and callbacks for Workflow Orchestrator.

The triggerer imports triggers and deadline callbacks by their dotted path,
from packages installed in its image. Code that only lives in the Git DAG
bundle isn't importable there, so these are installed into a custom image.
"""
