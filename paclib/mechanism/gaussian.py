"""
Contains logic for Gaussian Mechanism for adding noise
"""

from typing import NamedTuple
import numpy as np

from pacdb.budget_accountant.budget_accountant import BudgetAccountant


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

    return np.random.normal(0, scale=noise_to_add_variance)

def noise_to_add(avg_dist: float, c: float, budget: BudgetAccountant) -> float:
    """
    Returns a sample from a Gaussian distribution constructed from parameters
    Args:
        avg_dist (float): 
        c (float): 
        max_mi (float): Mutual Information value that bounds the noise

    Returns:
        float: Noise to be added
    """
    max_mi = budget.privacy_budget
    return noise_to_add_parameters(avg_dist, c, max_mi)
