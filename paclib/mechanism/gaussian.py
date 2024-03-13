"""
Contains logic for Gaussian Mechanism for adding noise
"""

from typing import NamedTuple


class GaussianDistribution(NamedTuple):
    mean: float
    variance: float

def noise_to_add_parameters(avg_dist: float, c: float, max_mi: float) -> GaussianDistribution:
    """
    Computes the mean and variance of the Gaussian distribution used by `noise_to_add`
    Args:
        avg_dist (float): 
        c (float): 
        max_mi (float): Mutual Information value that bounds the noise

    Returns:
        GaussianDistribution: The distribution from which to sample noise
    """
    # noise_to_add_mean = 0  # always 0
    noise_to_add_variance = ((avg_dist + c) / (2*(max_mi / 2.)))  # taken from PAC-ML code

    return GaussianDistribution(0, noise_to_add_variance)

def noise_to_add(avg_dist: float, c: float, max_mi: float) -> float:
    """
    Returns a sample from a Gaussian distribution constructed from parameters
    Args:
        avg_dist (float): 
        c (float): 
        max_mi (float): Mutual Information value that bounds the noise

    Returns:
        float: Noise to be added
    """
    return np.random.normal(0, noise_to_add_parameters(avg_dist, c, max_mi).variance)
