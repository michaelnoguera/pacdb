"""
Timer utility for measuring elapsed time and logging it to a CSV file.
"""

import csv
import logging
import os
import time
from dataclasses import asdict, dataclass, field
from io import TextIOWrapper
from typing import TYPE_CHECKING, TypeAlias

if TYPE_CHECKING:
    import _csv
    csvwriter: TypeAlias = _csv._writer
else:
    csvwriter: TypeAlias = any

@dataclass
class TimerConfig:
    """Configuration for the Timer, including experiment name and output location."""
    experiment: str = ""
    step: str = ""
    output_dir: str = "./times"
    output_file: str = field(init=False)
    
    def __post_init__(self):
        os.makedirs(self.output_dir, exist_ok=True)  # Ensure output directory exists
        self.output_file = f"{self.output_dir}/{self.experiment}.csv"  # Set output file path

@dataclass
class TimerState:
    """Internal state for the Timer."""
    label: str | None = None  # Label for the current timing
    start: float = 0.0        # Start time
    end: float = 0.0          # End time

class Timer:
    """Timer utility for measuring elapsed time and logging it to a CSV file."""
    def __init__(self, experiment: str, step: str, output_dir: str = "./times"):
        self.config = TimerConfig(experiment=experiment, step=step, output_dir=output_dir)
        self.state = TimerState()

        self._file_handle: TextIOWrapper = open(self.config.output_file, "a", newline="")  # File handle for CSV output
        self.writer: csvwriter = csv.writer(self._file_handle)  # CSV writer object

        if os.path.getsize(self.config.output_file) == 0:
            self.writer.writerow(["experiment", "step", "label", "elapsed_time"])  # Write header if file is new
            self._file_handle.flush()

    def start(self, label: str):
        self.state.label = label
        self.state.start = time.time()

    def end(self):
        self.state.end = time.time()
        self.writer.writerow([self.config.experiment, self.config.step, self.state.label, self.state.end-self.state.start])
        self.state.label = ""
        self.state.start, self.state.end = 0.0, 0.0

    @property
    def timer_context(self):
        """Return the current timer configuration and state as dictionaries."""
        return {
            "config": asdict(self.config),
            "state": asdict(self.state)
        }
    
    def __del__(self):
        if self._file_handle:
            self._file_handle.flush()
            self._file_handle.close()
    
if __name__ == "__main__":
    # Example usage
    timer = Timer(experiment="test_experiment", step="test_step")
    timer.start("test_label")
    time.sleep(2)  # Simulate some work
    elapsed = timer.end()