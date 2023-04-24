import pandas as pd
import numpy as np
import h5py
from typing import Dict, Optional, Any
from intervalframe import IntervalFrame

from .frame import Frame

    
class MultiFrame(object):
    def __init__(self,
                 frame_dict: Optional[Dict[str, Frame]] = None,
                 backend: str = "pandas"):
        """
        """

        # Set frame_dict
        if frame_dict is None:
            self.frames = {}
        else:
            self.frames = frame_dict
            #backend = [frame_dict[k].backend for k in frame_dict][0]
        
        # Set backend
        self.backend = backend

    
    def __repr__(self):
        return "MultiFrame with keys: "+", ".join(list(self.frames.keys()))


    def __getitem__(self, arg):
        try:
            return self.frames[arg]
        except KeyError:
            raise KeyError(arg+" not found in MultiFrame.")


    def __setitem__(self, arg, value):
        if isinstance(value, pd.DataFrame):
            self.frames[arg] = Frame.from_pandas(value)
        elif isinstance(value, Frame):
            self.frames[arg] = value
        else:
            raise TypeError("Value type not recognised.")

    
    def __len__(self):
        return len(self.frames)

    
    @property
    def keys(self):
        return set(self.frames.keys())


    @staticmethod
    def from_h5(h5_group: h5py.Group,
                backend: str = "pandas"):
        """
        """

        # Iterate over keys and read
        frame_dict = {}
        for key in list(h5_group.keys()):
            frame_dict[key] = Frame.from_h5(h5_group[key], backend)
        
        # Create MultiFrame
        mframe = MultiFrame(frame_dict, backend)

        return mframe

    
    def add_key(self,
                key: str):
        try:
            self.frames[key]
            #raise AttributeError("Key already present")
        except KeyError:
            self.frames[key] = Frame()


    def add_obs(self,
                obs: str | np.ndarray):
        """
        """

        # Check if str
        if isinstance(obs, str):
            obs = np.array([obs])

        # Add obs
        for key in self.keys:
            self.frames[key].add_obs(obs)

    
    def update(self,
               key: str,
               values: Frame | pd.DataFrame | pd.Series):
        """
        """

        try:
            self.frames[key].update(values)

        except KeyError:
            if isinstance(values, Frame):
                if values.backend == "pandas" and self.backend == "pandas":
                    self.frames[key] = values
                else:
                    raise TypeError("Value type not recognised.")
            
            elif isinstance(values, pd.DataFrame):
                if self.backend == "pandas":
                    self.frames[key] = Frame.from_pandas(values)
                else:
                    raise TypeError("Value type not recognised.")

            elif isinstance(values, pd.Series):
                if self.backend == "pandas":
                    self.frames[key] = Frame.from_pandas(values.to_frame().T)
                else:
                    raise TypeError("Value type not recognised.")


    def to_h5(self,
              h5_group: h5py.Group,
              compression_opts: int = 4):
        """
        """

        # Iterate over keys
        for key in self.keys:
            h5_key_group = h5_group.create_group(key)
            self.frames[key].to_h5(h5_key_group, compression_opts)


class MultiIntervalFrame(object):
    def __init__(self,
                 frame_dict: Optional[Dict[str, IntervalFrame]] = None):
        """
        """

        # Set frame_dict
        if frame_dict is None:
            self.frames = {}
        else:
            self.frames = frame_dict

    
    def __repr__(self):
        return "MultiIntervalFrame with keys: "+", ".join(list(self.frames.keys()))


    def __getitem__(self, arg):
        try:
            return self.frames[arg]
        except KeyError:
            raise KeyError(arg+" not found in MultiIntervalFrame.")


    def __setitem__(self, arg, value):
        if isinstance(value, IntervalFrame):
            self.frames[arg] = value
        else:
            raise TypeError("Value type not recognised.")

    
    def __len__(self):
        return len(self.frames)

    
    @property
    def keys(self):
        return set(self.frames.keys())

    
    @staticmethod
    def from_h5(h5_group: h5py.Group):
        """
        """

        # Iterate over keys and read
        iframe_dict = {}
        for key in list(h5_group.keys()):
            iframe_dict[key] = IntervalFrame.read_h5(h5_group[key])
        
        # Create MultiFrame
        mframe = MultiIntervalFrame(iframe_dict)

        return mframe

    
    def add_key(self,
                key: str):
        try:
            self.frames[key]
            raise AttributeError("Key already present")
        except KeyError:
            self.frames[key] = IntervalFrame()


    def add_obs(self,
                obs: str | np.ndarray):
        """
        """

        # Check if str
        if isinstance(obs, str):
            obs = np.array([obs])

        # Add obs
        for key in self.keys:
            key_obs = np.array(list(set(obs) - set(self.frames[key].df.columns.values)))
            for o in key_obs:
                self.frames[key].df.loc[:,o] = pd.Series([], dtype=object)


    def update(self,
               key: str,
               values: IntervalFrame):
        """
        """

        try:
            self.frames[key]
            values = values.exact_match(self.frames[key])
            self.frames[key] = self.frames[key].exact_match(values)
            for col in values.columns.values:
                self.frames[key].df.loc[:,col] = values.loc[:,col].values

        except KeyError:
            self.frames[key] = values


    def to_h5(self,
              h5_group: h5py.Group,
              compression_opts: int = 4):
        """
        """

        # Iterate over keys
        for key in self.keys:
            h5_key_group = h5_group.create_group(key)
            self.frames[key].to_h5(h5_key_group, compression_opts)

