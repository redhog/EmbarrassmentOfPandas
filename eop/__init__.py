import numpy as np
import pandas as pd
import types

class Filter(object):
    def __init__(self, row = None, col = None):
        self.row = row if row is not None else []
        self.col = col if col is not None else []

    def append(self, other):
        return Filter(self.row + other.row, self.col + other.col)

    def reset(self, row=False, col=False):
        return Filter(self.row if not row else Nonbe, self.col if not col else None)

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
    def row_position(self):
        if not self.row: return None
        last = self.row[-1]
        if isinstance(last, slice) and last.stop is False:
            return last.start
        return None

    @property
    def col_position(self):
        if not self.col: return None
        last = self.col[-1]
        if isinstance(last, slice) and last.stop is False:
            return last.start
        return None
    
class Container(object):
    def __init__(self, base = None, extension = None):
        assert (base is None) == (extension is None), "Both base and extension must be specified, or neither"
        self.base = base if base is not None else pd.DataFrame()
        self.extension = extension if extension is not None else pd.DataFrame([{}])

    def is_extension_col(self, col):
        return self.extension.loc[0, col] is not np.bool_(False)

        
class Instance(object):
    def __init__(self, data = None, filter = None):
        if isinstance(data, pd.DataFrame):
            extension = data.iloc[:1].reset_index(drop=True).astype("bool")
            extension[extension.columns] = np.bool_(False)
            data = Container(data, extension)
        self.data = data if data is not None else Container()
        self.filter = filter if filter is not None else Filter()

    def select(self, filter):
        filter = self.filter.append(filter)
        exts = filter.apply(self.data.extension, row=False)
        if isinstance(exts, pd.Series):
            ext = exts.iloc[0]
            if ext is np.bool_(False):
                # FIXME: Make a Column class to wrap here...
                return filter.apply(self.data.base)
            else:
                return ext.filter(filter.reset(col=True))
        else:
            return type(self)(self.data, filter)

    @property
    def filtered(self):
        filtered = self.filter.apply(self.data.base)
        return filtered.index, filtered.columns

    @property
    def base(self):
        return self.filter.apply(self.data.base)

    @property
    def extension(self):
        return self.filter.apply(self.data.extension, row=False)
        
    def assign(self, other):
        rows, cols = self.filtered
        other_rows, other_cols = other.filtered
        row_pos = self.filter.row_position
        col_pos = self.filter.col_position
        
        if col_pos is not None or not self.filter.col:
            for new_col in set(other_cols) - set(self.data.base.columns):
                if other.data.extension.loc[new_col, 0] is np.bool_(False):
                    self.data.base[new_col] = np.NaN
                    self.data.extension[new_col] = np.bool_(False)
                else:
                    dtype = type(other.data.extension[new_col].loc[0])
                    self.data.base[new_col] = np.bool_(False)
                    self.data.extension[new_col] = dtype(Container(pd.DataFrame([{} for x in range(len(self.data.base))]),
                                                                   pd.DataFrame([{} for x in range(len(self.data.base))])))

        other_base = other.data.base.loc[other_rows, other_cols]
        other_extension = other.data.extension[other_cols]

        if len(cols) and self.filter.col:
            assert len(cols) == len(other_cols), "Both instances must have the same number of columns"
            renames = dict(zip(other_cols, cols))
            other_base = other_base.rename(columns=renames)
            other_extension = other_extension.rename(columns=renames)
            
        def insert(df1, df2, pos):
            idx = self.data.base.index.get_loc(pos)
            return df1.iloc[:idx].append(df2).append(df1.iloc[idx:])

        if row_pos is not None:
            self.data.base = insert(self.data.base, other_base, row_pos)
        elif len(rows) == len(other_base):
            self.data.base.loc[rows, cols] = other_base
        else:
            self.data.base = insert(self.data.base.drop(rows), other_base, next(iter(rows)))
            
        for col in other_extension.columns:
            if col_pos is None and col not in self.data.extension.columns: continue
            if not self.data.is_extension_col(col): continue
            self.data.extension.loc[0, col].filter(self.filter.reset(col=True)).assign(other_extension.loc[0, col])

    
    # def format_header(self, row=0, colwidth=10):
    #     columns = [col if isinstance(col, tuple) else (col,) for col in self.data.base.columns]
        
    #     header = "|".join(
    #         self.data.extension.loc[0, col].format_header(row - len(col), colwidth)
    #         if self.data.is_extension_col(col)
    #         else (str(col) + " " * colwidth)[:colwidth]
    #         for col in columns)
        
    #     for row in 

    def __getitem__(self, item):
        col = item
        row = None
        if isinstance(item, tuple):
            row, col = (item + (None, None))[:2]
        item = Filter([row] if row is not None else [], [col] if col is not None else [])
        return self.select(item)
            
        
class A(Instance): pass
class B(Instance): pass
