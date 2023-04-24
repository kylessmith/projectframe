import pandas as pd
import numpy as np
import h5py
from ..engines.pandas_engine import PandasEngine
from typing import Dict, Optional, Any

class LocationIndexer(object):
    def __init__(self,
                 indexer: pd.core.indexing._LocIndexer):
        """
        """

        self.indexer = indexer


    def __getitem__(self, args):
        return self.indexer.__getitem__(args)


    def __setitem__(self, args, value):
        self.indexer.__setitem__(args, value)


class iLocationIndexer(object):
    def __init__(self,
                 indexer: pd.core.indexing._iLocIndexer):
        """
        """

        self.indexer = indexer


    def __getitem__(self, args):
        return self.indexer.__getitem__(args)
        

    def __setitem__(self, args, value):
        self.indexer.__setitem__(args, value)


class Frame(object):

    def __init__(self,
                 backend: str = "pandas",
                 engine: Optional[PandasEngine] = None):
        """
        """

        self.backend = backend
        if engine is None:
            engine = PandasEngine(pd.DataFrame([]))
        self.engine = engine

        if backend == "pandas":
            self.engine_type = PandasEngine
        else:
            raise NotImplementedError("Backend not implemented yet.")


    def __getitem__(self, args):
        
        return Frame(self.backend,
                     self.engine.__getitem__(args))

    
    def __setitem__(self, args, value):
        
        self.engine.__setitem__(args, value)
    
    def __repr__(self):
        return repr(self.engine)

    
    @property
    def loc(self):
        return LocationIndexer(self.engine.loc)

    @property
    def iloc(self):
        return iLocationIndexer(self.engine.iloc)

    @property
    def index(self):
        return self.engine.index

    @property
    def columns(self):
        return self.engine.columns


    @staticmethod
    def from_pandas(df: pd.DataFrame):
        return Frame("pandas", PandasEngine(df))

    
    @staticmethod
    def from_h5(h5_group: h5py.Group,
                backend: str = "pandas"):
        """
        """

        # Read h5py group
        if backend == "pandas":
            frame = Frame("pandas", PandasEngine.from_h5(h5_group))
        else:
            raise TypeError("Backend not supported...yet")

        return frame
        
    
    @property
    def values(self):
        return self.engine.values

    @property
    def shape(self):
        return self.engine.shape


    def add_obs(self,
                obs: str | np.ndarray):
        """
        """

        self.engine.add_obs(obs)

    
    def update(self,
               frame: pd.DataFrame):
        """
        """

        # Is Frame: pandas
        if isinstance(frame, Frame):
            if self.backend == "pandas" and frame.backend == "pandas":
                self.engine.update(frame.engine.df)
            else:
                raise TypeError("Frame backend doesn't match")

        elif isinstance(frame, pd.DataFrame):
            if self.backend == "pandas":
                self.engine.update(frame)
            else:
                raise TypeError("Frame backend doesn't match")

        elif isinstance(frame, pd.Series):
            if self.backend == "pandas":
                self.engine.update(frame.to_frame().T)
            else:
                raise TypeError("Frame backend doesn't match")

        else:
            raise TypeError("Unrecognised input.")


    def to_h5(self,
              h5_group: h5py.Group,
              compression_opts: int = 4):
        """
        """

        self.engine.to_h5(h5_group, compression_opts)

    
