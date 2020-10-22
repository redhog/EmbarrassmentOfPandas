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
        return "%s, %s" % ("".join("[%s]" % (item,) for item in self.row), "".join("[%s]" % (item,) for item in self.col))
    
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
        return type(self)(self.data, self.filter.append(filter))

    def unselect(self, row=False, col=False):
        return type(self)(self.data, self.filter.reset(row, col))
    
    def extract(self):
        if not self.filter.is_extractable:
            return self        
        filter = self.filter
        ext = filter.apply(self.data.extension, row=False).iloc[0]
        if ext is np.bool_(False):
            # FIXME: Make a Column class to wrap here...
            return filter.apply(self.data.base)
        else:
            return ext.select(filter.reset(col=True))

    @property
    def filtered(self):
        filtered = self.filter.apply(self.data.base)
        return filtered.index, filtered.columns

    @property
    def base_columns(self):
        return self.data.extension.loc[0].loc[self.data.extension.loc[0] == np.bool_(False)].index

    @property
    def extension_columns(self):
        return self.data.extension.loc[0].loc[self.data.extension.loc[0] != np.bool_(False)].index
    
    @property
    def base(self):
        return self.filter.apply(self.data.base)

    @property
    def extension(self):
        return self.filter.apply(self.data.extension, row=False)

    def wrap(self, col):
        return Instance(
            Container(
                pd.DataFrame({col:np.zeros(len(self.data.base)).astype("bool")}, index=self.data.base.index),
                pd.DataFrame({col:[self.unselect(row=True)]}))
        ).select(self.filter.reset(col=True))
    
    def assign(self, other):
        if self.filter.is_extractable:
            col = self.filter.col[-1]
            print("Extract", col)
            self.unselect(col=True).assign(
                other.wrap(col))
        else:
            rows, cols = self.filtered
            other_rows, other_cols = other.filtered
            row_pos = self.filter.row_position
            col_pos = self.filter.col_position

            if col_pos is not None or not self.filter.col:
                print("Include all columns")
                for new_col in set(other_cols) - set(self.data.base.columns):
                    if other.data.extension.loc[0, new_col] is np.bool_(False):
                        self.data.base[new_col] = np.NaN
                        self.data.extension[new_col] = np.bool_(False)
                    else:
                        dtype = type(other.data.extension.loc[0, new_col])
                        self.data.base[new_col] = np.bool_(False)
                        self.data.extension[new_col] = dtype(Container(pd.DataFrame([{} for x in range(len(self.data.base))]),
                                                                       pd.DataFrame([{} for x in range(len(self.data.base))])))

            other_base = other.data.base.loc[other_rows, other_cols]
            other_extension = other.data.extension[other_cols]

            if len(cols) and self.filter.col:
                print("Copy columns by position", cols)
                assert len(cols) == len(other_cols), "Both instances must have the same number of columns"
                renames = dict(zip(other_cols, cols))
                other_base = other_base.rename(columns=renames)
                other_extension = other_extension.rename(columns=renames)

            def insert(df1, df2, pos):
                idx = self.data.base.index.get_loc(pos)
                return df1.iloc[:idx].append(df2).append(df1.iloc[idx:])

            if row_pos is not None:
                print("Insert rows at", row_pos)
                self.data.base = insert(self.data.base, other_base, row_pos)
            elif len(rows) == len(other_base):
                print("Update rows", len(rows))
                self.data.base.loc[rows, cols] = other_base
            elif self.filter.row:
                print("Replace rows", len(rows), len(other_base))
                self.data.base = insert(self.data.base.drop(rows), other_base, next(iter(rows)))
            else:
                print("Merge rows", len(rows), len(other_base))
                existing = self.data.base.index & other_base.index
                new = (self.data.base.index ^ other_base.index) & other_base.index                
                self.data.base.loc[existing, cols] = other_base.loc[existing]
                self.data.base = self.data.base.append(other_base.loc[new])
                
            for col in other_extension.columns:
                if col not in self.data.extension.columns: continue
                if not self.data.is_extension_col(col): continue
                print("Assign extension column", col)
                self.data.extension.loc[0, col].select(self.filter.reset(col=True)).assign(other_extension.loc[0, col])

    
    # def format_header(self, row=0, colwidth=10):
    #     columns = [col if isinstance(col, tuple) else (col,) for col in self.data.base.columns]
        
    #     header = "|".join(
    #         self.data.extension.loc[0, col].format_header(row - len(col), colwidth)
    #         if self.data.is_extension_col(col)
    #         else (str(col) + " " * colwidth)[:colwidth]
    #         for col in columns)
        
    #     for row in 

    def __getitem__(self, item):
        return self.select(Filter.from_item(item)).extract()

    def __setitem__(self, item, value):
        self.select(Filter.from_item(item)).assign(value)
        
        
class A(Instance): pass
class B(Instance): pass
