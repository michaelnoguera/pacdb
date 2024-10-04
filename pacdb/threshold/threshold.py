"""
This module isolates the thresholding logic used by PACDataFrame.
"""

class Threshold():
    def __init__(self) -> None:
        self.threshold=23
        pass

    def set_threshold(self, threshold):
        self.threshold=threshold

    def check_threshold(self, size_of_sample) -> bool:
        if size_of_sample < self.threshold:
            return False
        else:
            return True