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
    
class Container(object):
    def __init__(self, base = None, extension = None, meta=None):
        if base is not None and extension is None:
            extension = base.iloc[:1].reset_index(drop=True).astype("bool")
            extension[extension.columns] = np.bool_(False)
        self.base = base if base is not None else pd.DataFrame()
        self.extension = extension if extension is not None else pd.DataFrame([{}])
        self.meta = meta if meta is not None else {}
        
    def is_extension_col(self, col):
        return self.extension.loc[0, col] is not np.bool_(False)

        
class Instance(object):
    DTypes = None
    Meta = None
    
    def __new__(cls, data = None, extension = None, meta=None, filter = None):
        if isinstance(data, Container):
            return cls.new_from_container(data, filter)

        self = cls.new_from_data(data, extension, meta, filter)
        if cls.DTypes is None:
            return self

        self2 = cls.new_empty()
        self2.assign(self)
        #self2.data.meta.update(meta)
        
        return self2

    @classmethod
    def new_from_data(cls, data = None, extension = None, meta=None, filter = None):
        self = object.__new__(cls)
        object.__setattr__(self, "filter", filter if filter is not None else Filter())
        if isinstance(data, dict):
            base = pd.DataFrame({key:value for key, value in data.items() if isinstance(value, list)})
            def instancify(key, value):
                if isinstance(value, Instance):
                    return value
                return cls.DTypes[key](value)
            extension = pd.DataFrame({key:[instancify(key, value)] for key, value in data.items() if isinstance(value, (Instance, dict))}, index=[0])
            basecols = list(base.columns)
            extcols = list(extension.columns)
            for key in extcols:
                base[key] = np.bool_(False)
            for key in basecols:
                extension[key] = np.bool_(False)
            object.__setattr__(self, "data", Container(base, extension, meta))
        else:
            object.__setattr__(self, "data", Container(data, extension, meta))
        return self
            
    @classmethod
    def new_from_container(cls, data, filter=None):
        self = object.__new__(cls)
        object.__setattr__(self, "filter", filter if filter is not None else Filter())
        if isinstance(data, Container):
            object.__setattr__(self, "data", data)
        return self
        
    @classmethod
    def new_empty(cls):
        base = None
        extension = None
        meta = None
        if cls.DTypes is not None:
            base_dtypes = cls.DTypes.map(lambda x: np.dtype('bool') if (type(x) is type and issubclass(x, Instance)) else x)
            base = pd.DataFrame(columns=base_dtypes.index).astype(base_dtypes)
            extension = pd.DataFrame(cls.DTypes.map(lambda x: x() if (type(x) is type and issubclass(x, Instance)) else np.bool_(False))).T
        if cls.Meta is not None:
            meta = dict(cls.Meta)
        self = object.__new__(cls)
        object.__setattr__(self, "data", Container(base, extension, meta))
        object.__setattr__(self, "filter", Filter())
        return self
        
    def select(self, filter):
        return type(self)(self.data, filter=self.filter.append(filter))

    def unselect(self, row=False, col=False):
        return type(self)(self.data, filter=self.filter.reset(row, col))
    
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
        res = self.filter.apply(self.data.base)
        return res[res.columns & self.base_columns]
        
    @property
    def extension(self):
        res = self.filter.apply(self.data.extension, row=False)
        res = res[res.columns & self.extension_columns]
        if len(res.columns) == 0: return res
        return pd.DataFrame(res.apply(
            lambda x:
            np.bool_(False)
            if x.loc[0] is np.bool_(False)
            else x.iloc[0].select(self.filter.reset(col=True)))).T

    def wrap(self, col):
        return Instance(
            Container(
                pd.DataFrame({col:np.zeros(len(self.data.base)).astype("bool")}, index=self.data.base.index),
                pd.DataFrame({col:[self.unselect(row=True)]}))
        ).select(self.filter.reset(col=True))
    
    def assign(self, other, prefix=()):
        def log(*arg):
            print(("  " * len(prefix)) + " ".join(str(item) for item in arg))
        log(".".join(str(item) for item in prefix) + ":")
        if self.filter.is_extractable:
            col = self.filter.col[-1]
            log("Extract", col)
            self.unselect(col=True).assign(
                other.wrap(col), prefix=prefix + ("Wrapped",))
        else:
            rows, cols = self.filtered
            other_rows, other_cols = other.filtered
            row_pos = self.filter.row_position
            col_pos = self.filter.col_position

            if col_pos is not None or not self.filter.col:
                log("Include all columns")
                for new_col in set(other_cols) - set(self.data.base.columns):
                    if other.data.extension.loc[0, new_col] is np.bool_(False):
                        self.data.base[new_col] = np.NaN
                        self.data.extension[new_col] = np.bool_(False)
                    else:
                        sub = other.data.extension.loc[0, new_col]
                        dtype = type(sub)
                        self.data.base[new_col] = np.bool_(False)
                        self.data.extension[new_col] = dtype(Container(pd.DataFrame([{} for x in range(len(self.data.base))]),
                                                                       pd.DataFrame([{} for x in range(len(self.data.base))]),
                                                                       dict(sub.data.meta)))

            other_base = other.data.base.loc[other_rows, other_cols]
            other_extension = other.data.extension[other_cols]

            if len(cols) and self.filter.col:
                log("Copy columns by position", cols)
                # FIXME: Handle replacing columns
                assert len(cols) == len(other_cols), "Both instances must have the same number of columns"
                renames = dict(zip(other_cols, cols))
                other_base = other_base.rename(columns=renames)
                other_extension = other_extension.rename(columns=renames)
            else:
                log("Update or columns", cols)
                cols = other_cols
                
            def insert(df1, df2, pos):
                idx = self.data.base.index.get_loc(pos)
                return df1.iloc[:idx].append(df2).append(df1.iloc[idx:])

            if row_pos is not None:
                log("Insert rows at", row_pos)
                self.data.base = insert(self.data.base, other_base, row_pos)
            elif len(rows) == len(other_base):
                log("Update rows", len(rows), cols)
                self.data.base.loc[rows, cols] = other_base
            elif self.filter.row:
                log("Replace rows", len(rows), len(other_base))
                self.data.base = insert(self.data.base.drop(rows), other_base, next(iter(rows)))
            else:
                log("Merge rows", len(rows), len(other_base))
                existing = self.data.base.index & other_base.index
                new = (self.data.base.index ^ other_base.index) & other_base.index                
                self.data.base.loc[existing, cols] = other_base.loc[existing]
                self.data.base = self.data.base.append(other_base.loc[new])
                
            for col in other_extension.columns:
                if col not in self.data.extension.columns: continue
                if not self.data.is_extension_col(col): continue
                log("Assign extension column", col)
                self.data.extension.loc[0, col].select(self.filter.reset(col=True)).assign(other_extension.loc[0, col], prefix=prefix+(col,))

            self.data.meta.update(other.data.meta)
                
    def flatten(self, prefix = (), include_types=False, include_first_filter=False):
        if include_types:
            t = type(self)
            f = ""
            if include_first_filter and (self.filter.col or self.filter.row):
                f = ": %s" % (self.filter,)
            prefix = prefix + ("<%s.%s%s>" % (t.__module__, t.__name__, f),)
        
        base = self.base
        base.columns = [prefix + (col if isinstance(col, tuple) else (col,)) for col in base.columns]
        extension = self.extension
        extension = [extension.loc[0, col].flatten(prefix + (col if isinstance(col, tuple) else (col,)), include_types)
                     for col in extension.columns]
        for ext in extension:
            for col in ext.columns:
                base[col] = ext[col]

        levels = base.columns.map(len).max()
        base.columns = pd.MultiIndex.from_tuples((col + (("",) * levels))[:levels] for col in base.columns)
        
        return base

    @property
    def dtypes(self):
        return self.base.dtypes.append(self.extension.loc[0].map(lambda x: type(x)))
    
    def __repr__(self):
        meta = "\n".join("%s: %s" % (key, str(value)[:30]) for key, value in self.data.meta.items())
        if meta:
            meta += "\n"
        content = repr(self.flatten(include_types=True, include_first_filter=True))
        return meta + content
        
    def __getitem__(self, item):
        return self.select(Filter.from_item(item)).extract()

    def __setitem__(self, item, value):
        self.select(Filter.from_item(item)).assign(value)

    def __delitem__(self, item):
        pass

    def __getattr__(self, name):
        try:
            return self.data.meta[name]
        except:
            raise AttributeError(name)
        
    def __setattr__(self, name, value):
        self.data.meta[name] = value
        
    def __delattr__(self, name):
        try:
            del self.data.meta[name]
        except:
            raise AttributeError(name)
    
class A(Instance): pass
class B(Instance): pass
class Point(Instance):
    DTypes = pd.Series({
        "x": np.dtype("float64"),
        "y": np.dtype("float64"),
        "z": np.dtype("float64")
    })
    Meta = {
        "crs": None
    }
class Measurement(Instance):
    DTypes = pd.Series({
        "pos": Point,
        "temp": np.dtype("float64"),
        "time": np.dtype("float64")
    })
