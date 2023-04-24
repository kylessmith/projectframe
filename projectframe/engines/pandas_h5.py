import pandas as pd
import h5py
import numpy as np
from intervalframe import IntervalFrame
from typing import Any, Dict, List, Optional
from pandas.api.types import is_float_dtype, is_integer_dtype, is_bool_dtype, is_list_like


def convert_recarray(rec_array: np.recarray) -> np.recarray:
    """
    Convert the dtypes of np.recarray to h5py compatible dtypes

    Parameters
    ----------
        rec_array : np.recarray
            numpy.recarray (Array to convert dtypes for)

    Returns
    -------
        rec_array : np.recarray
            numpy.recarray (Array with h5py compatible dtypes)
    """

    dtypes = {o:rec_array.dtype[o].str for o in rec_array.dtype.fields}
    for key_dtype in dtypes:
        if dtypes[key_dtype] == "|O":
            lengths = []
            for i in range(len(rec_array[key_dtype])):
                if isinstance(rec_array[key_dtype][i], str):
                    lengths.append(len(rec_array[key_dtype][i]))
                elif pd.isnull(rec_array[key_dtype][i]):
                    continue
                elif isinstance(rec_array[key_dtype][i], int):
                    continue
                elif isinstance(rec_array[key_dtype][i], float):
                    continue
                else:
                    continue
            max_length = max(lengths) if len(lengths) > 0 else 0
            if max_length > 0:
                dtypes[key_dtype] = "<S{}".format(max_length)
            else:
                dtypes[key_dtype] = "<f8"

    rec_array = rec_array.astype(list(dtypes.items()))

    return rec_array


def write_h5_DataFrame(h5_group: h5py.Group,
                       df: pd.DataFrame,
                       axis: int = 0,
                       compression_opts: int = 4) -> None:
    """
    Write pandas.DataFrame to h5py group

    Parameters
    ----------
        h5_group
            h5py.group
        df
            pandas.DataFrame
        axis
            int

    Returns
    -------
        None
    """
    
    # Record axis
    h5_group["axis"] = axis
    h5_group["shape"] = np.array(df.shape)

    # Determine is DataFrame in all float or integer
    if is_float_dtype(df.values) or is_integer_dtype(df.values):
        rec_array = df.values
        h5_group["typing"] = "numeric"
        # Write index
        index = df.index.values
        if index.dtype.str.startswith("|S")  or index.dtype.str.startswith("|O"):
            index = index.astype(bytes)
        h5_group["index"] = index
        # Write columns
        columns = df.columns.values
        if columns.dtype.str.startswith("|S") or columns.dtype.str.startswith("|O"):
            columns = columns.astype(bytes)
        h5_group["columns"] = columns
    
    # Iterate over columns
    elif axis == 1:
        if df.index.dtype.kind == "i":
            index_dtypes = "i"
        else:
            index_dtypes = "<S{}".format(df.index.str.len().max())
        if df.columns.dtype.kind == "i":
            columns_dtypes = "<S{}".format(df.columns.str.len().max())
        else:
            columns_dtypes = "<S{}".format(df.columns.str.len().max())

        rec_array = df.T.to_records(index_dtypes=index_dtypes)
        rec_array = convert_recarray(rec_array)
        h5_group["typing"] = "recarray"

    
    # Iterate over index
    elif axis == 0:
        if df.index.dtype.kind == "i":
            index_dtypes = "i"
        else:
            index_dtypes = "<S{}".format(df.index.str.len().max())
        if df.columns.dtype.kind == "i":
            columns_dtypes = "i"
        else:
            columns_dtypes = "<S{}".format(df.columns.str.len().max())

        rec_array = df.to_records(index_dtypes=index_dtypes)
        rec_array = convert_recarray(rec_array)
        h5_group["typing"] = "recarray"

    h5_group_values = h5_group.create_dataset("values", data=rec_array, compression="gzip",
                                              compression_opts=compression_opts, shape=rec_array.shape)

    return None


def write_h5_Series(h5_group: h5py.Group,
                       series: pd.Series,
                       compression_opts: int = 4) -> None:
    """
    Write pandas.DataFrame to h5py group

    Parameters
    ----------
        h5_group
            h5py.group
        df
            pandas.DataFrame

    Returns
    -------
        None
    """

    if series.is_monotonic:
        h5_group["typing"] = "series"
        if is_float_dtype(series) or is_integer_dtype(series) or is_bool_dtype(series):
            values = series.values
        elif series.dtype.str.startswith("|S")  or series.dtype.str.startswith("|O"):
            values = series.values.astype(bytes)
        
        h5_group_values = h5_group.create_dataset("values", data=values, compression="gzip",
                                                  compression_opts=compression_opts, shape=values.shape)

        if is_float_dtype(series.index.values) or is_integer_dtype(series.index.values):
            index = series.index.values
        elif series.index.values.dtype.str.startswith("|S")  or series.index.values.dtype.str.startswith("|O"):
            index = series.index.values.astype(bytes)
        
        h5_group_index = h5_group.create_dataset("index", data=index, compression="gzip",
                                                  compression_opts=compression_opts, shape=index.shape)
    
    else:
        raise TypeError("Series must be monotonic")


def read_h5_DataFrame(h5_group: h5py.Group) -> pd.DataFrame:
    """
    Read pandas.DataFrame from h5py group

    Parameters
    ----------
        h5_group : h5py.Group
            h5py.Group

    Returns
    -------
        df : pandas.DataFrame
            pandas.DataFrame
    """

    # Define dtype categories
    numeric_dtypes = set(['i','u','b','c'])
    string_dtypes = set(['O','U','S'])
    
    # Record axis
    axis = int(np.array(h5_group["axis"]))
    shape = np.array(h5_group["shape"])
    df_typing = str(np.array(h5_group["typing"]).astype(str))

    # If numeric
    if df_typing == "numeric":
        index = np.array(h5_group["index"])
        if index.dtype.str.startswith("|S") or index.dtype.str.startswith("|O"):
            index = index.astype(str)
        columns = np.array(h5_group["columns"])
        if columns.dtype.str.startswith("|S") or columns.dtype.str.startswith("|O"):
            columns = columns.astype(str)
        df = pd.DataFrame(np.array(h5_group["values"]),
                          index = np.array(h5_group["index"]).astype(str),
                          columns = np.array(h5_group["columns"]).astype(str))
    
    # Read index
    elif axis == 0:
        df = pd.DataFrame.from_records(np.array(h5_group["values"]), index="index")
        if df.index.dtype.kind in string_dtypes:
            df.index = df.index.values.astype(str)
        
    # Read columns
    elif axis == 1:
        df = pd.DataFrame.from_records(np.array(h5_group["values"]), index="index").T
        if df.columns.dtype.kind in string_dtypes:
            df.columns = df.columns.values.astype(str)

    # Convert numpy object to str
    for i, dtype in enumerate(df.dtypes):
        if dtype.kind == "O":
            df.iloc[:,i] = df.iloc[:,i].values.astype(str)
        
    return df


def read_h5_Series(h5_group: h5py.Group) -> pd.Series:
    """
    Read pandas.DataFrame from h5py group

    Parameters
    ----------
        h5_group : h5py.Group
            h5py.Group

    Returns
    -------
        df : pandas.Series
            pandas.Series
    """

    index = np.array(h5_group["index"])
    if index.dtype.str.startswith("|S"):
        index = index.astype(str)

    values = np.array(h5_group["values"])
    if values.dtype.str.startswith("|S"):
        values = values.astype(str)

    series = pd.Series(values, index=index)

    return series

