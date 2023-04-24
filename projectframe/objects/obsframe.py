import pandas as pd
import numpy as np
import h5py
from typing import Dict, Optional, Any
from intervalframe import IntervalFrame

# Local imports
from .frame import Frame
from .multiframe import MultiFrame, MultiIntervalFrame

    
class ObsFrame(object):
    def __init__(self,
                 frame_dict: Optional[Dict[str, MultiFrame]] = None,
                 backend: str = "pandas"):
        """
        """

        # Set frame_dict
        if frame_dict is None:
            self.frames_dict = {}
        else:
            self.frames_dict = frame_dict
        
        # Set backend
        self.backend = backend

    
    def __repr__(self):
        return "ObsFrame with obs: "+", ".join(list(self.frames_dict.keys()))


    def __getitem__(self,
                    arg: str):
                    
        try:
            return self.frames_dict[arg]
        except KeyError:
            raise KeyError(arg+" not found in MultiFrame.")


    def __setitem__(self,
                    arg: str,
                    value: MultiFrame):

        if isinstance(value, MultiFrame):
            self.frames_dict[arg] = value
        else:
            raise TypeError("Value type not recognised.")

    
    def __len__(self):
        return len(self.frames_dict)

    
    @property
    def obs(self):
        return set(self.frames_dict.keys())


    @staticmethod
    def from_h5(h5_group: h5py.Group,
                backend: str = "pandas"):
        """
        """

        # Iterate over keys and read
        frame_dict = {}
        for key in list(h5_group.keys()):
            frame_dict[key] = MultiFrame.from_h5(h5_group[key], backend)
        
        # Create MultiFrame
        mframe = ObsFrame(frame_dict, backend)

        return mframe

    
    def add_key(self,
                key: str):
        """
        """
        
        # Iterate over obs
        for obs in self.obs:
            self.frames_dict[obs].add_key(key)


    def add_obs(self,
                obs: str | np.ndarray):
        """
        """

        # Check if str
        if isinstance(obs, str):
            obs = np.array([obs])

        # Add obs
        for o in obs:
            try:
                self.frames_dict[o]
            except KeyError:
                self.frames_dict[o] = MultiFrame()

    
    def update(self,
               obs: str,
               key: str,
               values: Frame | pd.DataFrame | pd.Series):
        """
        """

        try:
            self.frames_dict[obs].update(key, values)
        
        except KeyError:
            self.frames_dict[obs] = MultiFrame()
            self.frames_dict[obs].update(key, values)

        print("updating", obs, key, values)


    def to_h5(self,
              h5_group: h5py.Group,
              compression_opts: int = 4):
        """
        """

        # Iterate over obs
        print("  ", h5_group.name)
        for obs in self.obs:
            print("   ", obs)
            h5_obs_group = h5_group.create_group(obs)
            if len(self.frames_dict[obs]) > 0:
                self.frames_dict[obs].to_h5(h5_obs_group, compression_opts)


class ObsIntervalFrame(object):
    def __init__(self,
                 frame_dict: Optional[Dict[str, IntervalFrame]] = None):
        """
        """

        # Set frame_dict
        if frame_dict is None:
            self.frames_dict = {}
        else:
            self.frames_dict = frame_dict

        
    def __repr__(self):
        return "ObsIntervalFrame with obs: "+", ".join(list(self.frames_dict.keys()))


    def __getitem__(self,
                    arg: str):
        try:
            return self.frames_dict[arg]

        except KeyError:
            raise KeyError(arg + " not found in MultiIntervalFrame.")


    def __setitem__(self,
                    arg: str,
                    value: MultiIntervalFrame):
        if isinstance(value, MultiIntervalFrame):
            self.frames_dict[arg] = value
        else:
            raise TypeError("Value type not recognised.")

    
    def __len__(self):
        return len(self.frames_dict)

    
    @property
    def obs(self):
        return set(self.frames_dict.keys())

    
    @staticmethod
    def from_h5(h5_group: h5py.Group):
        """
        """

        # Iterate over obs and read
        iframe_dict = {}
        for obs in list(h5_group.keys()):
            iframe_dict[obs] = MultiIntervalFrame.from_h5(h5_group[obs])
        
        # Create ObsIntervalFrame
        mframe = ObsIntervalFrame(iframe_dict)

        return mframe

    
    def add_key(self,
                key: str):
        try:
            self.frames_dict[key]
            raise AttributeError("Key already present")
        except KeyError:
            self.frames_dict[key] = IntervalFrame()


    def add_obs(self,
                obs: str | np.ndarray):
        """
        """

        # Check if str
        if isinstance(obs, str):
            obs = np.array([obs])

        # Add obs
        for o in obs:
            try:
                self.frames_dict[o]
            except KeyError:
                self.frames_dict[o] = MultiIntervalFrame()


    def update(self,
               obs: str,
               key: str,
               values: IntervalFrame):
        """
        """

        try:
            self.frames_dict[obs].update(key, values)

        except KeyError:
            self.frames_dict[obs] = MultiIntervalFrame()
            self.frames_dict[obs].update(key, values)


    def to_h5(self,
              h5_group: h5py.Group,
              compression_opts: int = 4):
        """
        """

        # Iterate over keys
        print("  ", h5_group.name)
        for obs in self.obs:
            print("   ", obs)
            h5_obs_group = h5_group.create_group(obs)
            if len(self.frames_dict[obs]) > 0:
                self.frames_dict[obs].to_h5(h5_obs_group, compression_opts)

