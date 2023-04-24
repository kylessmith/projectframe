import pandas as pd
import numpy as np
import h5py
from pandas.api.types import infer_dtype, is_list_like, is_integer, is_bool, is_re


def check_types(args):
    # Check types
    if is_list_like(args[0]) == False:
        if is_integer(args[0]):
            arg0_type = "integer"
        elif is_bool(args[0]):
            arg0_type = "boolean"
        elif isinstance(args[0], str):
            arg0_type = "string"
        elif isinstance(args[0], slice):
            arg0_type = "slice"
    else:
        arg0_type = infer_dtype(args[0])

    if is_list_like(args[1]) == False:
        if is_integer(args[1]):
            arg1_type = "integer"
        elif is_bool(args[1]):
            arg1_type = "boolean"
        elif isinstance(args[1], str):
            arg1_type = "string"
        elif isinstance(args[1], slice):
            arg1_type = "slice"
    else:
        arg1_type = infer_dtype(args[1])

    return arg0_type, arg1_type


def getitem(self, args):
    """
    """

    # Check args is of length two
    if len(args) != len(self.df.shape):
        raise IndexError("Wrong number of indexers.")

    # Check types
    arg0_type, arg1_type = check_types(args)

    # Is iterable
    if arg0_type == "integer":
        # (int,int), (int,bool), (int,slice)
        if arg1_type == "integer" or arg1_type == "boolean" or arg1_type == "slice":
            return PandasEngine(self.df.iloc[args[0], args[1]])
        # (int,str)
        elif arg1_type == "string":
            return PandasEngine(self.df.iloc[args[0],:].loc[:,args[1]])
    
    elif arg0_type == "string":
        # (str,str), (str,bool), (str,slice)
        if arg1_type == "string" or arg1_type == "boolean" or arg1_type == "slice":
            return PandasEngine(self.df.loc[args[0], args[1]])
        # (str,int)
        if arg1_type == "integer":
            return PandasEngine(self.df.loc[args[0],:].iloc[:,args[1]])


def setitem(self, args, value):
    # Check args is of length two
    if len(args) != len(self.df.shape):
        raise IndexError("Wrong number of indexers.")

    # Check types
    arg0_type, arg1_type = check_types(args)

    # Is iterable
    if arg0_type == "integer":
        # (int,int), (int,bool), (int,slice)
        if arg1_type == "integer" or arg1_type == "boolean" or arg1_type == "slice":
            self.df.iloc[args[0], args[1]] = value
        # (int,str)
        elif arg1_type == "string":
            self.df.iloc[args[0],:].loc[:,args[1]] = value
    
    elif arg0_type == "string":
        # (str,str), (str,bool), (str,slice)
        if arg1_type == "string" or arg1_type == "boolean" or arg1_type == "slice":
            self.df.loc[args[0], args[1]] = value
        # (str,int)
        if arg1_type == "integer":
            self.df.loc[args[0],:].iloc[:,args[1]] = value


class PandasEngine(object):
    def __init__(self,
                 df: pd.DataFrame):
        self.df = df

    def __repr__(self):
        return repr(self.df)

    def __getitem__(self, args):
        
        # Pass to pandas
        self.df.__getitem__(args)

    
    def __setitem__(self, args, value):
        # Check args is of length two
        self.df.__setitem__(args, value)
    

    @staticmethod
    def from_h5(h5_group: h5py.Group):
        """
        """
        from .pandas_h5 import read_h5_DataFrame
        
        if len(h5_group) > 0:
            engine = PandasEngine(read_h5_DataFrame(h5_group))
        else:
            engine = PandasEngine(pd.DataFrame([]))

        return engine


    @property
    def values(self):
        return self.df.values

    @property
    def shape(self):
        return self.df.shape

    @property
    def iloc(self):
        return self.df.iloc

    @property
    def loc(self):
        return self.df.loc

    @property
    def index(self):
        return self.df.index.values

    @property
    def columns(self):
        return self.df.columns.values


    def update(self,
               values: pd.DataFrame):
        """
        """

        self.df = self.df.combine_first(values)

    
    def add_obs(self,
                obs: str | np.ndarray):
        """
        """

        if isinstance(obs, str):
            obs = np.array([obs])
        obs = np.array(list(set(obs) - set(self.df.index.values)))
        new_index = np.append(self.df.index.values, obs)
        self.df = self.df.reindex(new_index)

    
    def to_h5(self,
              h5_group: h5py.Group,
              compression_opts: int = 4):
        """
        """
        from .pandas_h5 import write_h5_DataFrame
        
        write_h5_DataFrame(h5_group, self.df, axis=0, compression_opts=compression_opts)