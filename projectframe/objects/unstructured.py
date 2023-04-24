import pandas as pd
import numpy as np
import h5py
from typing import Dict, Optional, Any
from pandas.api.types import is_list_like
from intervalframe import IntervalFrame

# Local imports
from ..engines.pandas_h5 import write_h5_DataFrame, write_h5_Series, read_h5_DataFrame, read_h5_Series


def process_dict(key: str,
                 value: Any,
                 rewriteable: bool = True,
                 uns: Any = None):
    """
    """

    # Initialize
    if uns is None:
        uns = UnstructuredLookup(rewriteable = rewriteable)

    # Check key is str
    if isinstance(key, str):
        if uns.rewriteable == False:
            try:
                uns.dict[key]
                if uns.dict[key] == value:
                    return
                else:
                    raise AttributeError("Conflicting value assignments.")
            except KeyError:
                pass
        
    # Input key, value pair
    if is_list_like(value) and not isinstance(value, dict):
        # numpy array
        if isinstance(value, np.ndarray):
            uns.dict[key] = value
        # List
        elif isinstance(value, list):
            uns.dict[key] = np.array(value)
        # set
        elif isinstance(value, set):
            uns.dict[key] = np.array(list(value))
        # pd.Series
        elif isinstance(value, pd.Series):
            uns.dict[key] = value
        # Other
        else:
            raise TypeError(str(type(value)) + " not supported.")

    elif isinstance(value, int) or isinstance(value, float) or isinstance(value, str):
        uns.dict[key] = np.array(value)

    elif isinstance(value, pd.DataFrame) or isinstance(value, IntervalFrame):
        uns.dict[key] = value

    elif isinstance(value, dict):
        uns.dict[key] = UnstructuredLookup(rewriteable = rewriteable)
        for key2 in value:
            process_dict(key2, value[key2], rewriteable, uns.dict[key])

     # Other
    else:
        raise TypeError(str(type(value)) + " not supported.")
    
    return uns


def write_h5_dict(h5_group: h5py.Group,
                  dictionary: Dict[str, Any]):
    """
    """

    # Iterate over dictionary keys
    for key in dictionary:

        # dict
        if isinstance(dictionary[key], UnstructuredLookup):
            h5_key = h5_group.create_group(key)
            write_h5_dict(h5_key, dictionary[key].dict)

        # np.ndarray
        elif isinstance(dictionary[key], np.ndarray):
            if dictionary[key].dtype.str.startswith("<U")  or dictionary[key].dtype.str.startswith("|O"):
                h5_group[key] = dictionary[key].astype(bytes)
            else:
                h5_group[key] = dictionary[key]

        # pd.DataFrame
        elif isinstance(dictionary[key], pd.DataFrame):
            h5_group_key = h5_group.create_group(key)
            write_h5_DataFrame(h5_group_key, dictionary[key])

        # pd.Series
        elif isinstance(dictionary[key], pd.Series):
            h5_group_key = h5_group.create_group(key)
            write_h5_Series(h5_group_key, dictionary[key])

        # IntervalFrame
        elif isinstance(dictionary[key], IntervalFrame):
            h5_group_key = h5_group.create_group(key)
            dictionary[key].to_h5(h5_group_key)

        else:
            raise TypeError(str(type(dictionary[key])) + " not supported")


def read_h5_dict(h5_group: h5py.Group,
                 dictionary: Optional[Dict] = {}):
    """
    """

    # Iterate over dictionary keys
    for key in list(h5_group.keys()):

        # Check is dict
        if isinstance(h5_group[key], h5py.Group):
            if "intervals" in list(h5_group[key].keys()):
                value = IntervalFrame.read_h5(h5_group[key])

            elif "typing" in list(h5_group[key].keys()):
                h5_type = np.array(h5_group[key]["typing"]).astype(str)
                if h5_type == "series":
                    value = read_h5_Series(h5_group[key])
                else:
                    value = read_h5_DataFrame(h5_group[key])
            else:
                value = {}
                read_h5_dict(h5_group[key], value)

        # np.ndarray
        else:
            value = np.array(h5_group[key])
            if value.dtype.str.startswith("|S"):
                value = value.astype(str)
        
        dictionary[key] = value


class UnstructuredLookup(object):
    """
    """
    
    def __init__(self,
                 dictionary: Optional[Dict] = None,
                 rewriteable: bool = True):
        """
        """
        
        if dictionary is None:
            self.dict = {}
        else:
            self.dict = dictionary

        self.rewriteable = rewriteable


    def __getitem__(self,
                    key: str) -> np.ndarray:
        """
        """

        return self.dict[key]


    def __setitem__(self,
                    key: str,
                    value: Any):
        """
        """

        process_dict(key, value, self.rewriteable, self)


    def __repr__(self):
        return repr(self.dict)

    
    @property
    def keys(self):
        return list(self.dict.keys())

    @property
    def values(self):
        return list(self.dict.values())


    @staticmethod
    def from_h5(h5_group: h5py.Group):
        """
        """

        # Iterate over keys and read
        uns = UnstructuredLookup()
        read_h5_dict(h5_group, uns.dict)

        return uns


    def to_h5(self,
              h5_group: h5py.Group):
        """
        """

        write_h5_dict(h5_group, self.dict)