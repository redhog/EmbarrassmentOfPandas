import numpy as np
import pandas as pd
import types


class DataInstanceLoc(object):
    def __init__(self, di):
        self.di = di

    def __getitem__(self, key):
        res = self.di.df.loc[key]
        if isinstance(res, pd.Series):
            res = pd.DataFrame(res).T
        return self.di.type.instantiate(res)

    def __setitem__(self, key, value):
        if isinstance(value, DataInstance):
            value = value.df
        self.di.df.loc[key] = value

class DataType(object):
    def __init__(self, cls = None, dtypes = None, attributes = None):
        self.cls = cls
        self.dtypes = dtypes or {}
        self.attributes = attributes or {}

    def instantiate(self, df):
        cls = self.cls or DataInstance
        return cls(df, self.dtypes, **self.attributes)
            
    def copy(self):
        return type(self)(self.cls, copy_dtypes(self.dtypes), dict(self.attributes))
        
    def __repr__(self):
        if self.cls is None:
            return repr(self.dtypes)
        else:
            return "%s(%s)" % (self.cls.__name__, ", ".join("%s=%s" % (name, repr(value)) for name, value in self.dtypes.items()))
        
def copy_dtypes(dtypes):
    if isinstance(dtypes, dict):
        return {name:copy_dtypes(value) for name, value in dtypes.items()}
    elif isinstance(dtypes, DataType):
        return dtypes.copy()
    else:
        return dtypes

def select_dtypes(key, dtypes):
    if isinstance(key, list):
        return {name:value
                for name, value in ((item[0] if isinstance(item, tuple) else item,
                                     select_dtypes(item, dtypes))
                                    for item in key)
                if value is not None}
    elif key in dtypes:
        return dtypes[key]
    elif isinstance(key, tuple):
        for item in key:
            res = select_dtypes(item, dtypes)
            if res is None:
                return None
            if isinstance(res, DataType):
                dtypes = res.dtypes
            else:
                dtypes = None
        return res
    else:
        return None

    
class DataContainer(object):
    def __init__(self, df, dtypes, attributes):
        if df is None:
            df = pd.DataFrame({})
            df.index = pd.MultiIndex([[]], [[]])
        elif not isinstance(df.columns, pd.MultiIndex):
            df.columns = pd.MultiIndex.from_tuples((name,) for name in df.columns)
        self.df = df
        self.dtypes = dtypes
        self.attributes = attributes or {}
        
    def getsubset(self, rowfilter=None, colfiler=None):
        return FilterContainer(self, rowfiler, colfilter)
    
class FilterContainer(object):
    def __init__(self, data, rowfilter, colfilter):
        self.data = data
        self.rowfilter = rowfilter
        self.colfiler = colfilter

    @property
    def df(self):
        if self.rowfiler is not None and self.colfilter is not None:
            return self.data.df.loc[self.rowfilter, self.colfiler]
        elif self.rowfiler is not None:
            return self.data.df.loc[self.rowfilter]
        else:
            return self.data.df[self.colfilter]
        
    @property
    def dtypes(self):
        return select_dtypes(self.colfilter, self.data.dtypes)
        
    @property
    def attributes(self):
        return self.data.attributes
                               
                               
class DataInstance(object):
    DTypes = {}

    def __init__(self, df = None, dtypes = None, data = None, **attributes):
        if data is None:
            self.data = DataContainer(
                df,
                dtypes or copy_dtypes(type(self).DTypes),
                attributes)
            self._enforce_dtypes()
            self._save_dtypes()
        else:
            self.data = data

    @property
    def type(self):
        return DataType(type(self), self.data.dtypes, self.data.attributes)
        
    def _save_dtypes(self):
        for key, dtype in self.data.df.dtypes.items():
            self._save_dtype(key, dtype)
            
    def _save_dtype(self, key, dtype):
        while key and key[-1] in (None, np.NaN):
            key = key[:-1]
        dtypes = self.data.dtypes
        for item in key[:-1]:
            if item not in dtypes or not isinstance(dtypes[item], DataType):
                dtypes[item] = DataType()
            dtypes = dtypes[item].dtypes
        dtypes[key[-1]] = dtype

    def _enforce_dtypes(self, prefix = (), dtypes = None):
        if dtypes is None: dtypes = self.data.dtypes
        for key, value in dtypes.items():
            if isinstance(value, DataType):
                self._enforce_dtypes(prefix + (key,), value.dtypes)
            else:
                col = prefix + (key,)
                if col not in self.data.df:
                    self.data.df[col] = np.nan
                self.data.df = self.data.df.astype({col: value})

    def _clean_key(self, key):
        if isinstance(key, list):
            return [self._clean_key(item) for item in key]
        elif not isinstance(key, tuple):
            return (key,)
        return key
        
    def __getitem__(self, key):
        key = self._clean_key(key)
        res = self.data.df[key]
        t = select_dtypes(key, self.data.dtypes)
        if isinstance(t, (dict, DataType)) and isinstance(res, pd.Series):
            res = pd.DataFrame({key: res}).T
        if isinstance(t, dict):
            return type(self)(res, t)
        elif isinstance(t, DataType):
            return t.instantiate(res)
        else:
            return res

    def _adapt_key(self, key):
        other_keylen = len(key)
        self_keylen = len(self.data.df.columns.levels)
        if other_keylen > self_keylen:
            if len(self.data.df.columns):
                pad = ("",) * (other_keylen - self_keylen)
                self.data.df.columns = pd.MultiIndex.from_tuples(col + pad for col in self.data.df.columns)
            else:
                self.data.df.columns = pd.MultiIndex([[""] for l in range(other_keylen)], [[] for l in range(other_keylen)])
        elif self_keylen > other_keylen:
            pad = ("",) * (self_keylen - other_keylen)
            key = key + pad
        return key

    def _adapt_keys(self, keys):
        return [self._adapt_key(key) for key in keys]
    
    def __setitem__(self, key, value):
        key = self._clean_key(key)
        if isinstance(value, DataInstance):
            assert not isinstance(key, list)
            dtypes = self.data.dtypes
            for item in key[:-1]:
                if item not in dtypes:
                    dtypes[item] = DataType(DataInstance, {})
                dtypes = dtypes[item].dtypes
            dtypes[key[-1]] = value.type.copy()
            value = value.data.df
        if isinstance(value, pd.DataFrame) and not isinstance(key, list):
            other_columns = [key + col for col in value.columns]
            other_columns = self._adapt_keys(other_columns)
            self.data.df[other_columns] = value
        else:
            if isinstance(key, list):
                key = self._adapt_keys(key)
            else:
                key = self._adapt_key(key)
            self.data.df[key] = value
            if not isinstance(key, list): key = [key]
            for subkey in key:
                self._save_dtype(subkey, self.data.df.dtypes[subkey])

    def __delitem__(self, name):
        key = self._clean_key(name)
        del self.data.df[name]
        if not isinstance(name, list): name = [name]
        for item in name:
            dtypes = self.data.dtypes
            for part in item[:-1]:
                if part not in dtypes: break
                if not isinstance(dtypes[part], DataType): break
                dtypes = dtypes[part].dtypes
            else:
                del dtypes[item[-1]]
                
            
    def wrap(self, value):
        if isinstance(value, pd.DataFrame):
            return type(self)(value, copy_dtypes(self.data.dtypes))
        return value
    
    def __getattr__(self, name):
        if name in ("df", "dtypes", "attributes"):
            return getattr(self.data, name)
        if name in self.data.attributes:
            return self.data.attributes[name]
        attr = getattr(self.data.df, name)
        if isinstance(attr, types.MethodType):
            def wrapper(*arg, **kw):
                return self.wrap(attr(*arg, **kw))
            return wrapper
        else:
            return self.wrap(attr)

    def __setattr__(self, name, value):
        if name in ("data"):
            object.__setattr__(self, name, value)
        elif name in ("df", "dtypes", "attributes"):
            setattr(self.data, name, value)
        else:
            self.data.attributes[name] = value
        
    def __delattr__(self, name):
        del self.data.attributes[name]

    @property
    def loc(self):
        return DataInstanceLoc(self)

    def __repr__(self):
        t = type(self)
        return "%s.%s:\n%s" % (t.__module__, t.__name__, repr(self.data.df))

    def summary(self):
        res = DataInstance(self.data.df.copy(), copy_dtypes(self.data.dtypes))
        for name, dtype in self.data.dtypes.items():
            del res[name]
            if isinstance(dtype, DataType):
                if dtype.cls is None:
                    pass
                else:
                    res[(name, "[%s]" % (dtype.cls.__name__,))] = self[name].summary()
            else:
                res[(name, "[%s]" % (dtype,))] = self[name]
        return res
