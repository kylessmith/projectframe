from __future__ import absolute_import
from .core.core import ProjectFrame
from .objects.frame import Frame
from .objects.multiframe import MultiFrame, MultiIntervalFrame
from .objects.unstructured import UnstructuredLookup
from .objects.obsframe import ObsFrame, ObsIntervalFrame


# This is extracted automatically by the top-level setup.py.
__version__ = '1.0.0'
__author__ = "Kyle S. Smith"


__doc__ = """\
API
======

Basic class
-----------

.. autosummary::
   :toctree: .
   
   ProjectFrame
   Frame
   MultiFrame
   MultiIntervalFrame
   ObsFrame
   ObsIntervalFrame
   UnstructuredLookup
    
"""