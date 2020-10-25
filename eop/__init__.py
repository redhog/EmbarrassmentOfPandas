import numpy as np
import pandas as pd
import types

from . import special_numeric
from . import filter as filtermod
from . import container as containermod
from . import datainstance as datainstancemod
from . import dataset as datasetmod

Filter = filtermod.Filter
DataInstance = datainstancemod.DataInstance
Tag = datasetmod.Tag
DataSet = datasetmod.DataSet

class A(DataInstance): pass
class B(DataInstance): pass
class Point(DataInstance):
    DTypes = pd.Series({
        "x": np.dtype("float64"),
        "y": np.dtype("float64"),
        "z": np.dtype("float64")
    })
    Meta = {
        "crs": None
    }
class Measurement(DataInstance):
    DTypes = pd.Series({
        "pos": Point,
        "temp": np.dtype("float64"),
        "time": np.dtype("float64")
    })
