import numpy as np
from typing import NamedTuple

class GaussianDistribution(NamedTuple):
    mean: float
    variance: float

    def sample(self) -> float:
        """
        Returns a sample from the Gaussian distribution
        """
        return np.random.normal(self.mean, self.variance)

def noise_to_add(avg_dist: float, c: float, max_mi: float) -> GaussianDistribution:
    """
    Returns the mean and variance of the Gaussian distribution used by `noise_to_add`.
    You can call `sample` on the returned object to get a sample from the distribution.
    """
    # noise_to_add_mean = 0  # always 0
    noise_to_add_variance = ((avg_dist + c) / (2*(max_mi / 2.)))  # taken from PAC-ML code

    return GaussianDistribution(0, noise_to_add_variance)
