import numpy as np
import pandas as pd
import types
    
class Container(object):
    def __init__(self, base = None, extension = None, meta=None):
        if base is not None and extension is None:
            extension = base.iloc[:1].reset_index(drop=True).astype("bool")
            extension[extension.columns] = np.bool_(False)
        self.base = base if base is not None else pd.DataFrame()
        self.extension = extension if extension is not None else pd.DataFrame([{}], index=[0])
        self.meta = meta if meta is not None else {}
        
    def is_extension_col(self, col):
        return self.extension.loc[0, col] is not np.bool_(False)
