import numpy as np
import pandas as pd
import types
from . import datainstance

class Container(object):
    def __init__(self, base = None, extension = None, meta=None):
        if base is not None and extension is None:
            extension = base.iloc[:1].reset_index(drop=True).astype("bool")
            extension[extension.columns] = np.NaN
        self.base = base if base is not None else pd.DataFrame()
        self.extension = extension if extension is not None else pd.DataFrame([{}], index=[0])
        self.meta = meta if meta is not None else {}
        
    def is_extension_col(self, col):
        val = self.extension.loc[0, col]
        return isinstance(val, datainstance.DataInstance)
