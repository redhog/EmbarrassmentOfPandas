import numpy as np
import pandas as pd
import types

class Filter(object):
    def __init__(self, row = None, col = None):
        self.row = row if row is not None else []
        self.col = col if col is not None else []

    @classmethod
    def from_item(cls, item):
        col = item
        row = None
        if isinstance(item, tuple):
            row, col = (item + (None, None))[:2]
        return cls([row] if row is not None else [], [col] if col is not None else [])
        
    def append(self, other):
        return Filter(self.row + other.row, self.col + other.col)

    def reset(self, row=False, col=False):
        return Filter(self.row if not row else None, self.col if not col else None)

    def apply(self, df, row=True, col=True):
        if col:
            for f in self.col:
                if isinstance(f, slice) and f.stop is False:
                    df = df[[]]
                else:
                    df = df[f]
        if row:
            for f in self.row:
                if isinstance(f, slice) and f.stop is False:
                    df = df.iloc[0:0]
                else:
                    df = df.loc[f]
        return df

    @property
    def is_extractable(self):
        return self.col and not isinstance(self.col[-1], (list, np.ndarray, slice, pd.Index))
    
    @property
    def row_position(self):
        if not self.row: return None
        last = self.row[-1]
        if isinstance(last, slice) and last.stop is False:
            return last.start
        elif last == []:
            return -1
        return None

    @property
    def col_position(self):
        if not self.col: return None
        last = self.col[-1]
        if isinstance(last, slice) and last.stop is False:
            return last.start
        elif last == []:
            return -1
        return None

    def __repr__(self):
        def stritem(item):
            if isinstance(item, slice):
                res = [part if part is not None else "" for part in [item.start, item.stop, item.step]]
                if not res[-1]: res = res[:1]
                return ":".join(res)
            elif isinstance(item, list):
                return ",".join(str(part) for part in item)
            return str(item)
        return "%s | %s" % ("/".join(stritem(item) for item in self.row), "/".join(stritem(item) for item in self.col))
