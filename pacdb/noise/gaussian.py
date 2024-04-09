"""
This module isolates the noise generation and addition logic used by PACDataFrame.
"""

import numpy as np
from typing import NamedTuple

class GaussianDistribution(NamedTuple):
    """
    Struct representing a Gaussian distribution. Used for storing information about 
    distributions in a structured way. Provides a method to sample from the distribution,
    which is used for generating noise.
    """
    mean: float
    variance: float

    def sample(self) -> float:
        """
        Returns a sample from the Gaussian distribution
        """
        return np.random.normal(self.mean, self.variance)

def noise_to_add(avg_dist: float, c: float, max_mi: float) -> GaussianDistribution:
    """
    Returns the distribution from which noise should be sampled during a PAC release.
    You can call `sample` on the returned object to get a sample from the distribution.

    Example:
    ```
    Yj = pac_lung_df._applyQuery(pac_lung_df.sampler.sample())
    noise = pac_lung_df.noise_distribution.sample()
    noised_Yj = Yj + noise
    ```
    """
    noise_to_add_variance = ((avg_dist + c) / (2*(max_mi / 2.)))  # taken from PAC-ML code

    return GaussianDistribution(0, noise_to_add_variance)
