#import os
from typing import Any, Dict, List
import numpy as np
import pandas as pd
import h5py
#import argparse
from intervalframe import IntervalFrame

from ..objects.frame import Frame
from ..objects.multiframe import MultiFrame, MultiIntervalFrame
from ..objects.unstructured import UnstructuredLookup
from ..objects.obsframe import ObsFrame, ObsIntervalFrame
#from dataclasses import dataclass, field


class ProjectFrame(object):
    """
    Object to store all data for a project
    """

    def __init__(self,
                anno: Frame = None,
                values: MultiFrame = None,
                obs_values: ObsFrame = None,
                intervals: MultiFrame  = None,
                obs_intervals: ObsIntervalFrame = None,
                obs: set = None,
                uns: UnstructuredLookup = None,
                params: UnstructuredLookup = None):
        """
        Object to store all data for a project

        Parameters
        ----------
            anno : Frame
                Annotation data
            values : MultiFrame
                Values for all observations
            obs_values : ObsFrame
                Values for each observation
            intervals : MultiIntervalFrame
                Intervals for all observations
            obs_intervals : ObsIntervalFrame
                Intervals for each observation
            obs : set
                Set of observations
            uns : UnstructuredLookup
                Unstructured data
            params : UnstructuredLookup
                Parameters

        Returns
        -------
            None

        """

        # Assign everything
        ## NOTE: If assigned in default, causes shared object
        ##       issue when creating new ProjectFrames 
        ##       e.g o1.anno == o2.anno
        if anno is None:
            self.anno = Frame()
        else:
            self.anno = anno

        if values is None:
            self.values = MultiFrame()
        else:
            self.values = values

        if obs_values is None:
            self.obs_values = ObsFrame()
        else:
            self.obs_values = obs_values
        
        if intervals is None:
            self.intervals = MultiIntervalFrame()
        else:
            self.intervals = intervals

        if obs_intervals is None:
            self.obs_intervals = ObsIntervalFrame()
        else:
            self.obs_intervals = obs_intervals
        
        if obs is None:
            self.obs = set([])
        else:
            self.obs = obs

        if uns is None:
            self.uns = UnstructuredLookup(rewriteable=True)
        else:
            self.uns = uns
        
        if params is None:
            self.params = UnstructuredLookup(rewriteable=False)
        else:
            self.params = params


    def __len__(self):
        """
        Get length from self.frags
        """

        return len(self.obs)


    def __repr__(self):
        repr_string = "anno: " + str(self.anno.shape) + "\n"
        repr_string += "values: " + str(len(self.values)) + "\n"
        repr_string += "obs_values: " + str(len(self.obs_values)) + "\n"
        repr_string += "intervals: " + str(len(self.intervals)) + "\n"
        repr_string += "obs_intervals: " + str(len(self.obs_intervals)) + "\n"

        return repr_string

    @staticmethod
    def from_h5(h5_group: h5py.Group | str):
        """
        Read ProjectFrame from h5py.Group or file name

        Parameters
        ----------
            h5_group : h5py.Group or str
                h5py.Group or file name

        Returns
        -------
            pframe : ProjectFrame
                ProjectFrame object
        """

        # Check if file name
        is_file = False
        if isinstance(h5_group, str):
            f = h5py.File(h5_group, "r")
            h5_group = f["projectframe"]
            is_file = True

        # Read
        anno = Frame.from_h5(h5_group["anno"])
        values = MultiFrame.from_h5(h5_group["values"])
        obs_values = ObsFrame.from_h5(h5_group["obs_values"])
        intervals = MultiIntervalFrame.from_h5(h5_group["intervals"])
        obs_intervals = ObsIntervalFrame.from_h5(h5_group["obs_intervals"])
        obs = set(np.array(h5_group["obs"]).astype(str))
        uns = UnstructuredLookup.from_h5(h5_group["uns"])
        params = UnstructuredLookup.from_h5(h5_group["params"])

        # Close file
        if is_file:
            f.close()

        pframe = ProjectFrame(anno, values, obs_values,
                              intervals, obs_intervals, obs,
                              uns, params)

        
        return pframe

    
    def add_obs(self,
                obs: str | np.ndarray):
        """
        Add observations to ProjectFrame

        Parameters
        ----------
            obs : str or np.ndarray
                Observation name(s)

        Returns
        -------
            None
        """

        # Add obs
        if isinstance(obs, str):
            obs = np.array([obs])

        # Remove previous obs
        #obs = np.array(list(set(obs) - self.obs))
        
        # Add obs
        anno_obs = np.array(list(set(obs) - self.obs))
        self.obs.update(anno_obs)

        # Update all frames
        self.anno.add_obs(obs)

        self.values.add_obs(obs)
        self.intervals.add_obs(obs)
            
        for o in obs:
            self.obs_values.add_obs(o)
            self.obs_intervals.add_obs(o)


    def add_anno(self,
                 key: str,
                 obs: str | None = None,
                 value: str | float | int = np.nan):
        """
        Add value to key in .anno

        Parameters
        ----------
            key : str
                Key for .anno columns
            obs : str
                Observation file name
            value : str, float, or int
                Values to record

        Returns
        -------
            None

        """

        # Add obs
        if obs is None:
            self.anno.loc[:,key] = value
        else:
            self.anno.loc[obs, key] = value


    def add_intervals(self,
                      key: str,
                      values: IntervalFrame):
        """
        Add value to key in .anno

        Parameters
        ----------
            key : str
                Key for .anno columns
            values : IntervalFrame
                Values to record

        Returns
        -------
            None

        """

        # Update
        self.intervals.update(key, values)

        # Update obs
        self.add_obs(values.columns.values)

        
    def add_obs_intervals(self,
                          obs: str,
                          key: str,
                          values: IntervalFrame):
        """
        Add value to key in .anno

        Parameters
        ----------
            obs : str
                Key for .anno columns
            key : str
                Key for .anno columns
            values : IntervalFrame
                Values to record

        Returns
        -------
            None

        """

        # Update
        self.obs_intervals[obs].update(key, values)

        # Update obs
        self.add_obs(obs)


    def add_obs_values(self,
                       obs: str,
                       key: str,
                       values: pd.DataFrame | pd.Series):
        """
        Add value to key in .anno

        Parameters
        ----------
            obs : str
                Key for .anno columns
            key : str
                Key for .anno columns
            values : np.array or pandas.DataFrame
                Values to record

        Returns
        -------
            None

        """

        # Check name
        if isinstance(values, pd.Series):
            if values.name is None:
                raise AttributeError("Series is missing .name")

        # Update
        self.obs_values.update(obs, key, values)

        # Update obs
        self.add_obs(obs)
    

    def add_values(self,
                   key: str,
                   values: pd.DataFrame | pd.Series):
        """
        Add value to key in .anno

        Parameters
        ----------
            key : str
                Key for .anno columns
            values : np.array or pandas.DataFrame
                Values to record

        Returns
        -------
            None

        """

        # Check name
        if isinstance(values, pd.Series):
            if values.name is None:
                raise AttributeError("Series is missing .name")

        # Update
        self.values.update(key, values)

        # Update obs
        if isinstance(values, pd.DataFrame):
            self.add_obs(values.index.values)
        elif isinstance(values, pd.Series):
            self.add_obs(values.name)


    def to_h5(self,
              h5_group: h5py.Group | str,
              compression_opts: int = 4,
              verbose: bool = False):
        """
        """

        # Detemine if h5 group of file name
        is_file = False
        if isinstance(h5_group, str):
            if verbose: print("Creating h5 file:", h5_group)
            import h5py
            f = h5py.File(h5_group, "w")
            h5_group = f.create_group("projectframe")
            is_file = True

        # Write obs
        if verbose: print("Writing", len(self.obs),"obs")
        obs = np.array(list(self.obs)).astype(str)
        h5_group["obs"] = obs.astype(bytes)

        # Write uns
        if verbose: print("Writing uns")
        h5_uns = h5_group.create_group("uns")
        self.uns.to_h5(h5_uns)

        if verbose: print("Writing parameters")
        h5_params = h5_group.create_group("params")
        self.params.to_h5(h5_params)

        # Iterate over attributes
        if verbose: print("Writing anno")
        h5_anno = h5_group.create_group("anno")
        self.anno.to_h5(h5_anno, compression_opts)

        if verbose: print("Writing obs_values")
        h5_obs_values = h5_group.create_group("obs_values")
        self.obs_values.to_h5(h5_obs_values, compression_opts)

        if verbose: print("Writing obs_intervals")
        h5_obs_intervals = h5_group.create_group("obs_intervals")
        self.obs_intervals.to_h5(h5_obs_intervals, compression_opts)

        if verbose: print("Writing intervals")
        h5_intervals = h5_group.create_group("intervals")
        self.intervals.to_h5(h5_intervals, compression_opts)

        if verbose: print("Writing values")
        h5_values = h5_group.create_group("values")
        self.values.to_h5(h5_values, compression_opts)

        # Close file
        if is_file:
            if verbose: print("Done. Closing file.")
            f.close()
