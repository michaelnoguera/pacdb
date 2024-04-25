from dataclasses import dataclass
from typing import Any, Callable, List, Optional, Tuple, Union

import numpy as np
import pyspark.mllib.linalg.distributed
import pyspark.pandas as ps
import pyspark.sql.types as T
from pyspark import RDD, SparkContext
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.linalg import DenseMatrix, DenseVector, Vectors
from pyspark.sql import DataFrame

from .sampler import DataFrameSampler, SamplerOptions


@dataclass
class PACOptions:
    """
    Options for the PAC algorithm.
    """
    trials: int = 1000
    """number of trials for PAC algorithm, will determine length of X and Y"""
    max_mi: float = 1./8
    """maximum mutual information allowed for the query"""
    c: float = 0.001
    """security parameter, lower bound for noise added"""
    anisotropic: bool = False
    """use anisotropic noise estimation"""

class PACDataFrame:
    """
    Create a PACDataFrame from a PySpark DataFrame to use PAC-private functions.
    A new PACDataFrame will have a new DataFrameSampler attached to it.

    Example:
    ```
    from pacdb import PACDataFrame, SamplerOptions

    pac_lung_df = (PACDataFrame.fromDataFrame(lung_df)
                    .withSamplerOptions(
                        SamplerOptions(
                            withReplacement=False, 
                            fraction=0.5
                        )
                    ))
    ```
    """

    def __init__(self, df: DataFrame):
        """
        Construct a new PACDataFrame from a PySpark DataFrame. Use `fromDataFrame` instead.
        """
        self.df = df
        self.sampler: DataFrameSampler = DataFrameSampler(self.df)
        self.options = PACOptions()
        self.query: Callable[..., DataFrame] | None = None  # set by withQuery

    @classmethod
    def fromDataFrame(cls, df: DataFrame) -> "PACDataFrame":
        """
        Create a PACDataFrame from an existing Spark DataFrame.
        """
        return cls(df)
    
    def withOptions(self, options: PACOptions) -> "PACDataFrame":
        """
        Set the PAC options for the dataframe.
        """
        self.options = options
        return self

    ### Sampler methods ###

    def withSamplerOptions(self, options: SamplerOptions) -> "PACDataFrame":
        """
        Set the sampling options for the attached sampler
        """
        if self.sampler is None:
            raise ValueError("No sampler attached to this dataframe")
        self.sampler = self.sampler.withOptions(options)  # type: ignore  # TODO: fix abstract type error
        return self


    ### PAC inputs ###

    def setNumberOfTrials(self, trials: int) -> "PACDataFrame":
        """
        Set the number of trials to be used by the PAC algorithm. This is used to compute the privacy
        guarantee, and should be set to a large number for accurate results.
        """
        self.trials = trials
        return self
    
    def setMutualInformationBound(self, max_mi: float) -> "PACDataFrame":
        """
        Sets `self.max_mi`, used by `_estimate_noise`.
        """
        self.max_mi = max_mi
        return self


    ### PAC algorithm ###

    def _produce_one_sampled_output(self) -> DenseVector:
        X: DataFrame = self.sampler.sample()
        Y: DataFrame = self._applyQuery(X)
        output: DenseVector = self._unwrapDataFrame(Y)
        return output
    
    @staticmethod
    def sample_once_static(pacdf: "PACDataFrame") -> DenseVector:
        X: DataFrame = pacdf.sampler.sample()
        Y: DataFrame = pacdf._applyQuery(X)
        output: DenseVector = pacdf._unwrapDataFrame(Y)
        return output
    
    @staticmethod
    def estimate_hybrid_noise_static(
            sample_once: Callable[[], DenseVector],
            max_mi: float = 1./4,
            anisotropic: bool = False,
            eta: float = 0.05
            ) -> Tuple[List[float], List[Any]]:
        

        # Use the identity matrix for our projection matrix
        dimensions = len(sample_once())
        proj_matrix: np.ndarray = np.eye(dimensions)

        # If no projection matrix is supplied, compute one
        BASIS_SAMPLES = 500
        if anisotropic:
            sc = SparkContext.getOrCreate()

            # 1. COLLECT SAMPLES
            outputs: RDD[DenseVector] = sc.parallelize(
                [sample_once() for _ in range(BASIS_SAMPLES)]
            )

            # 2. COVARIANCE MATRIX
            mat: pyspark.mllib.linalg.distributed.RowMatrix = pyspark.mllib.linalg.distributed.RowMatrix(outputs.map(lambda x: (x,)))  # type: ignore[arg-type, return-value]
            y_cov: pyspark.mllib.linalg.Matrix = mat.computeCovariance()
            y_cov_T: np.ndarray = y_cov.toArray().T

            # 3. PROJECTION MATRIX (from SVD of covariance matrix)
            y_cov_rdd = sc.parallelize(y_cov_T)
            y_cov_distmatrix = pyspark.mllib.linalg.distributed.RowMatrix(y_cov_rdd)
            svd: pyspark.mllib.linalg.distributed.SingularValueDecomposition = y_cov_distmatrix.computeSVD(y_cov_distmatrix.numCols(), computeU=False)
            V: DenseMatrix = svd.V

            proj_matrix = V.toArray().T
        else:
            proj_matrix = np.eye(dimensions)

        # projected samples used to estimate variance in each basis direction
        # est_y[i] is a list of magnitudes of the outputs in the i-th basis direction
        est_y: List[List[np.ndarray]] = [[] for _ in range(dimensions)]
        prev_ests: List[Union[float, np.floating[Any]]] = [np.inf for _ in range(dimensions)] # only to measure change per iteration for convergence

        converged = False
        curr_trial = 0

        while not converged:
            output: np.ndarray = sample_once().toArray()
            assert len(output) == dimensions

            # Compute the magnitude of the output in each of the basis directions, update the estimate lists
            for i in range(len(output)):
                if anisotropic:
                    est_y[i].append(np.matmul(proj_matrix[i].T, output.T)) # transform back to original basis before storing
                else:
                    est_y[i].append(output[i])

            # Every 10 trials, check for convergence
            if curr_trial % 10 == 0:
                # If all dimensions' variance estimates changed by less than eta, we have converged
                if all(abs(np.var(est_y[i]) - prev_ests[i]) <= eta for i in range(dimensions)):
                    converged = True
                else:
                    # we have not converged, so update the previous estimates and continue
                    prev_ests = [np.var(est_y[i]) for i in range(dimensions)]
            curr_trial += 1

        # Now that we have converged, get the variance in each basis direction
        fin_var: List[np.floating[Any]] = [np.var(est_y[i]) for i in range(dimensions)]
        # and the mean in each basis direction
        fin_mean: List[np.floating[Any]] = [np.mean(est_y[i]) for i in range(dimensions)]

        sqrt_total_var = sum(fin_var)**0.5

        noise: List[float] = [np.inf for _ in range(dimensions)]
        for i in range(dimensions):
            noise[i] = float(1./(2*max_mi) * fin_var[i]**0.5 * sqrt_total_var)

        return noise, [sqrt_total_var, fin_var, fin_mean]
    

    def _estimate_hybrid_noise(
        self,
        max_mi: Optional[float] = None,
        anisotropic: Optional[bool] = None,
        quiet: bool = False
        ) -> List[float]:
        """
        Use the hybrid algorithm to determine how much noise to add to each dimension of the query result.

        Parameters:
        max_mi: float, optional
            Maximum mutual information allowed for the query. If not provided, use the PACDataFrame setting.
        anisotropic: bool, optional
            Whether to use anisotropic noise estimation. If not provided, use the PACDataFrame setting (default False).
        
        Returns:
        noise: List[float]
            How much noise to add to each dimension of the query result vector. The value in noise[i] is the
            variance of a Gaussian distribution from which to sample noise to add to the i-th dimension of the
            query result.
        """

        if max_mi is None:  # optional argument, otherwise use PACDataFrame setting
            max_mi = self.options.max_mi

        if anisotropic is None:
            anisotropic = self.options.anisotropic

        eta: float = 0.05  # convergence threshold  # TODO what should eta be?

        return self.estimate_hybrid_noise_static(
            self._produce_one_sampled_output,
            max_mi=max_mi,
            anisotropic=anisotropic,
            eta=eta)[0]
    

    @staticmethod
    def _add_noise(result: np.ndarray, noise: List[float], quiet=False) -> np.ndarray:
        # noise is an array of variances, one for each dimension of the query result
        noised = []
        for i in range(len(result)):
            noised.append(result[i] + np.random.normal(0, noise[i]))

        if not quiet: 
            print(f'Sample: {result} + Noise = Noised: {noised}')
        
        return np.array(noised)

    def releaseValue(self, quiet=False) -> DataFrame:
        """
        Execute the query with PAC privacy.
        """
        
        X: DataFrame = self.sampler.sample()
        Y: DataFrame = self._applyQuery(X) 
        output: np.ndarray = self._unwrapDataFrame(Y).toArray()

        if not quiet:
            print("Found output format of query: ")
            zeroes: np.ndarray = np.zeros(output.shape)
            self._updateDataFrame(Vectors.dense(zeroes), Y).show()

        noise: List[float] = self._estimate_hybrid_noise()

        noised_output: np.ndarray = self._add_noise(output, noise, quiet=quiet)

        output_df = self._updateDataFrame(Vectors.dense(noised_output), Y)

        if not quiet:
            print("Inserting to dataframe:")
            output_df.show()
        
        return output_df


    ### Query methods ###

    def withQuery(self, query_function: Callable[..., DataFrame]) -> "PACDataFrame":
        """
        Set the query function to be made private.
        """
        self.query = query_function
        return self
    
    def _applyQuery(self, df: DataFrame) -> DataFrame:
        """
        Directly apply the query to the given dataframe and return the exact output. This is not private at all!
        """

        return df.transform(self.query)  # type: ignore[arg-type]
    
    @staticmethod
    def _unwrapDataFrame(df: DataFrame) -> DenseVector:
        """
        Convert a PySpark DataFrame into a numpy vector.
        This is the same as "canonicalizing" the data as described in the PAC-ML paper.
        """
        
        numeric_columns: List[str] = [f.name for f in df.schema.fields if isinstance(f.dataType, T.NumericType)]

        assembler = VectorAssembler(inputCols=numeric_columns, outputCol="features", handleInvalid="error")
        df_vector = assembler.transform(df).select("features").rdd.flatMap(lambda x: x.features)

        return Vectors.dense(df_vector.collect())

    @staticmethod
    def _updateDataFrame(vec: DenseVector, df: DataFrame) -> DataFrame:
        """
        Use the values of the vector to update the PySpark DataFrame.
        """

        # Recompute shape and columns
        numeric_columns: List[str] = [f.name for f in df.schema.fields if isinstance(f.dataType, T.NumericType)]
        shape = (df.count(), len(numeric_columns))
        
        # Convert flat-mapped array to an array of rows
        np_array = np.reshape(vec.toArray(), shape)

        # -> Pandas-On-Spark (attach column labels)
        new_pandas: ps.DataFrame = ps.DataFrame(np_array, columns=numeric_columns)

        # Merge the new values with the old DataFrame
        old_pandas: ps.DataFrame = df.pandas_api()
        old_pandas.update(new_pandas)
        updated_df: DataFrame = old_pandas.to_spark()

        return updated_df