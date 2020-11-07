import numpy as np
import pandas as pd
import types
import sys

from . import special_numeric
from . import filter as filtermod
from . import container as containermod
from . import pdutils

class Loc(object):
    def __init__(self, instance):
        self.instance = instance
        
    def __getitem__(self, item):
        if not isinstance(item, tuple): item = (item,None)
        return self.instance.select(filtermod.Filter.from_item(item)).extract()

    def __setitem__(self, item, value):
        if not isinstance(item, tuple): item = (item,None)
        self.instance.select(filtermod.Filter.from_item(item)).assign(value)

class NoAction(object): pass
        
class DataInstance(special_numeric.SpecialNumeric):
    DTypes = None
    Meta = None

    DEBUG = False
    
    def __new__(cls, data = None, base = None, extension = None, meta=None, filter = None):
        if isinstance(data, containermod.Container):
            return cls.new_from_container(data, filter)

        self = cls.new_from_data(data, base, extension, meta, filter)
        if cls.DTypes is None:
            return self

        self2 = cls.new_empty()
        self2.assign(self)
        #self2.data.meta.update(meta)
        
        return self2

    @classmethod
    def new_from_data(cls, data = None, base = None, extension = None, meta=None, filter = None):
        self = object.__new__(cls)
        object.__setattr__(self, "filter", filter if filter is not None else filtermod.Filter())

        if isinstance(data, pd.DataFrame):
            if isinstance(data.columns, pd.MultiIndex):
                data = {col: (cls.DTypes if cls.DTypes is not None else {}).get(col, DataInstance)(data[col])
                        if isinstance(data[col], pd.DataFrame)
                        else data[col]
                        for col in data.columns.levels[0]}
            else:
                base = data
        
        if isinstance(data, dict):
            base = pd.DataFrame({key:value for key, value in data.items() if isinstance(value, (list, np.ndarray, pd.Series))})
            def instancify(key, value):
                if isinstance(value, DataInstance):
                    return value
                return cls.DTypes[key](value)
            extension = pd.DataFrame({key:[instancify(key, value)] for key, value in data.items() if isinstance(value, (DataInstance, dict))}, index=[0])
            basecols = list(base.columns)
            extcols = list(extension.columns)
            for key in extcols:
                base[key] = np.NaN
            for key in basecols:
                extension[key] = np.NaN
        object.__setattr__(self, "data", containermod.Container(base, extension, meta))
        return self
            
    @classmethod
    def new_from_container(cls, data, filter=None):
        self = object.__new__(cls)
        object.__setattr__(self, "filter", filter if filter is not None else filtermod.Filter())
        if isinstance(data, containermod.Container):
            object.__setattr__(self, "data", data)
        return self
        
    @classmethod
    def new_empty(cls, index=None):
        base = None
        extension = None
        meta = None
        if cls.DTypes is not None:
            dtypes = cls.DTypes
            if not isinstance(dtypes, pd.Series): dtypes = pd.Series(dtypes)
            base_dtypes = dtypes.map(lambda x: np.dtype('bool') if (type(x) is type and issubclass(x, DataInstance)) else x)
            base = pd.DataFrame(columns=base_dtypes.index, index=index).astype(base_dtypes)
            extension = pd.DataFrame(dtypes.map(lambda x: x.new_empty(index=index)
                                                if (type(x) is type and issubclass(x, DataInstance))
                                                else np.NaN)).T
        if cls.Meta is not None:
            meta = dict(cls.Meta)
        self = object.__new__(cls)
        object.__setattr__(self, "data", containermod.Container(base, extension, meta))
        object.__setattr__(self, "filter", filtermod.Filter())
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
        if isinstance(ext, DataInstance):
            return ext.select(filter.reset(col=True))
        else:
            # FIXME: Make a Column class to wrap here...
            return filter.apply(self.data.base)

    @property
    def filtered(self):
        filtered = self.filter.apply(self.data.base)
        return filtered.index, filtered.columns

    @property
    def base_columns(self):
        return self.data.extension.loc[0].loc[self.data.extension.loc[0].isna()].index

    @property
    def extension_columns(self):
        return self.data.extension.loc[0].loc[~self.data.extension.loc[0].isna()].index
    
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
            np.NaN
            if x.loc[0] is np.NaN
            else x.iloc[0].select(self.filter.reset(col=True)))).T

    def wrap(self, col):
        return DataInstance(
            containermod.Container(
                pd.DataFrame(columns=[col], index=self.data.base.index),
                pd.DataFrame({col:[self.unselect(row=True)]}))
        ).select(self.filter.reset(col=True))
    
    def assign(self, other, rename=False, prefix=()):
        def log(*arg):
            if self.DEBUG:
                sys.stderr.write("%s\n" % (("  " * len(prefix)) + " ".join(str(item) for item in arg),))
                sys.stderr.flush()
        log("%s: [%s] = [%s/%s]" % (".".join(str(item) for item in prefix) + ":", self.filter, other.base.columns, other.base.index))
        
        if self.filter.is_extractable:
            col = self.filter.col[-1]
            log("Extract", col)
            self.unselect(col=True).assign(
                other.wrap(col), rename=rename, prefix=prefix + ("Wrapped",))
        else:
            rows, cols = self.filtered
            
            other_base = other.filter.apply(other.data.base)
            other_extension = other.filter.apply(other.data.extension)
            
            if not self.filter.col:
                cols = other_base.columns.intersection(self.data.base.columns)
            if not self.filter.row:
                rows = other_base.index.intersection(self.data.base.index)

            # FIXME: Handle column/row rename here...
            self.data.base = pdutils.square_assign(self.data.base, rows, cols, other_base, rename)

            self.data.extension = pdutils.square_assign(
                self.data.extension, pd.Index([0]), cols, pd.DataFrame(columns=other_extension.columns, index=[0]), rename)

            if rename:
                other_extension.columns = cols
            
            columns_to_add = other_extension.columns.difference(cols)
            
            for new_col in columns_to_add:
                if other.data.is_extension_col(new_col):
                    sub = other_extension.loc[0, new_col]
                    dtype = type(sub)
                    self.data.extension[new_col] = dtype.new_empty(index=self.data.base.index)

            for col in other_extension.columns:
                if not self.data.is_extension_col(col): continue
                log("Assign extension column", col, self.data.extension.loc[0, col])
                self.data.extension.loc[0, col].select(
                    self.filter.reset(col=True)
                ).assign(
                    other.extension.loc[0, col], rename=rename, prefix=prefix+(col,))

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

        if len(base.columns):
            levels = base.columns.map(len).max()
            base.columns = pd.MultiIndex.from_tuples((col + (("",) * levels))[:levels] for col in base.columns)
        else:
            base.columns = pd.MultiIndex(codes=[[]], levels = [[]])
        
        return base

    @property
    def dtypes(self):
        dtypes = self.data.base.dtypes.copy()
        extcols = ~self.data.extension.loc[0].isna()
        dtypes[extcols] = self.data.extension.loc[0][extcols].map(lambda x: type(x))
        dtypes = pd.DataFrame(dtypes).T
        return self.filter.reset(row=True).apply(dtypes).loc[0]

    @dtypes.setter
    def dtypes(self, dtypes):
        pass
    
    @property
    def columns(self):
        rows, cols = self.filtered
        return cols
    
    @columns.setter
    def columns(self, columns):
        if self.filter.col:
            rows, cols = self.filtered
            columns = self.data.base.columns.map(
                lambda x: columns[cols.get_loc(x)] if x in cols else x)
        self.data.base.columns = columns
        self.data.extension.columns = columns

    @property
    def index(self):
        rows, cols = self.filtered
        return rows

    @index.setter
    def index(self, index):
        if self.filter.col:
            rows, cols = self.filtered
            index = self.data.base.columns.map(
                lambda x: index[rows.get_loc(x)] if x in rows else x)
        self.data.base.index = index
        def set_index(item):
            if isinstance(item, DataInstance):
                item.index = index
        self.data.extension.loc[0].map(set_index)
    
    def __repr__(self):
        meta = "\n".join("%s: %s" % (key, str(value)[:30]) for key, value in self.data.meta.items())
        if meta:
            meta += "\n"
        content = repr(self.flatten(include_types=True, include_first_filter=True))
        return meta + content

    @property
    def loc(self):
        return Loc(self)
    
    def __getitem__(self, item):
        return self.select(filtermod.Filter.from_item(item)).extract()

    def __setitem__(self, item, value):
        if value is NoAction: return
        selected = self.select(filtermod.Filter.from_item(item))
        selected.assign(value)

    def __delitem__(self, item):
        pass

    def __ilshift__(self, value):
        self.assign(value, rename = True)
        return NoAction
    
    def __getattr__(self, name):
        if name in self.data.meta:
            return self.data.meta[name]
        attr = getattr(self.base, name)
        if isinstance(attr, types.MethodType):
            def wrapper(*arg, **kw):
                filter = self.filter.reset(col=True)
                base = filter.apply(self.data.base)
                base = getattr(base, name)(*arg, **kw)
                def apply_item(item):
                    if isinstance(item, DataInstance):
                        return getattr(item.select(filter), name)(*arg, **kw)
                    return item
                if isinstance(base, pd.Series):
                    base = pd.DataFrame(base).T
                if isinstance(base, pd.DataFrame):
                    extension = pd.DataFrame(self.data.extension.loc[0].map(apply_item)).T
                    return type(self)(containermod.Container(base, extension, dict(self.data.meta))).select(self.filter.reset(row=True))
                return base
            return wrapper
        else:
            return attr

    def __setattr__(self, name, value):
        if name in ("data", "filter", "columns", "index"):
            super(DataInstance, self).__setattr__(name, value)
        else:
            self.data.meta[name] = value
        
    def __delattr__(self, name):
        try:
            del self.data.meta[name]
        except:
            raise AttributeError(name)
        
