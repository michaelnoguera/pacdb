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

        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s | %(filename)s:%(lineno)d %(levelname)s %(message)s"
        )

    def start(self, label: str):
        """Start timing with a given label."""
        logging.debug(f"Starting timer {label}")
        self.state.label = label
        self.state.start = time.time()

    def end(self) -> float:
        """Stop timing and record the elapsed time and corresponding label. Writes to CSV."""
        logging.debug(f"Ending timer {self.state.label}")
        if not self.state.label:
            raise ValueError("Timer has not been started. Call start() before end().")
        self.state.end = time.time()
        elapsed_time = self.state.end - self.state.start
        logging.info(f"Elapsed time for {self.state.label}: {elapsed_time:.2f} seconds")

        assert self.writer is not None, "TimerState writer is not initialized"
        self.writer.writerow([self.config.experiment, self.config.step, self.state.label, elapsed_time])
        
        return elapsed_time

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