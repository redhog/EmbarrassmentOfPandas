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
        return type(self.di)(res, self.di.dtypes)

    def __setitem__(self, key, value):
        if isinstance(value, DataInstance):
            value = value.df
        self.di.df[key] = value

class DataType(object):
    def __init__(self, cls, dtypes):
        self.cls = cls
        self.dtypes = dtypes

    def instantiate(self, df):
        cls = self.cls or DataInstance
        return cls(df, self.dtypes)
            
    def copy(self):
        return type(self)(self.cls, copy_dtypes(self.dtypes))
        
    def __repr__(self):
        if self.cls is None:
            return repr(self.dtypes)
        else:
            return "%s(%s)" % (self.cls, repr(self.dtypes))
        
def copy_dtypes(dtypes):
    if isinstance(dtypes, dict):
        return {name:copy_dtypes(value) for name, value in dtypes.items()}
    elif isinstance(dtypes, DataType):
        return dtypes.copy()
    else:
        return dtypes

class DataInstance(object):
    dtypes = {}
    
    def __init__(self, df = None, dtypes = None):
        if df is None:
            df = pd.DataFrame({})
            df.index = pd.MultiIndex([[]], [[]])
        elif not isinstance(df.columns, pd.MultiIndex):
            df.columns = pd.MultiIndex.from_tuples((name,) for name in df.columns)
        self.df = df
        self.dtypes = dtypes or copy_dtypes(type(self).dtypes)
        self._enforce_dtypes()
        self._save_dtypes()

    def _save_dtypes(self):
        for key, dtype in self.df.dtypes.items():
            self._save_dtype(key, dtype)
            
    def _save_dtype(self, key, dtype):
        while key and key[-1] in (None, np.NaN):
            key = key[:-1]
        dtypes = self.dtypes
        for item in key[:-1]:
            if item not in dtypes or not isinstance(dtypes[item], DataType):
                dtypes[item] = DataType(None, {})
            dtypes = dtypes[item].dtypes
        dtypes[key[-1]] = dtype

    def _enforce_dtypes(self, prefix = (), dtypes = None):
        if dtypes is None: dtypes = self.dtypes
        for key, value in dtypes.items():
            if isinstance(value, DataType):
                self._enforce_dtypes(prefix + (key,), value.dtypes)
            else:
                col = prefix + (key,)
                if col not in self.df:
                    self.df[col] = np.nan
                self.df = self.df.astype({col: value})

    def _get_columns(self, key, dtypes = None):
        if dtypes is None:
            dtypes = self.dtypes
        
        if isinstance(key, list):
            return {name:value
                    for name, value in ((item[0] if isinstance(item, tuple) else item,
                                         self._get_columns(item, dtypes))
                                        for item in key)
                    if value is not None}
        elif key in dtypes:
            return dtypes[key]
        elif isinstance(key, tuple):
            for item in key:
                res = self._get_columns(item, dtypes)
                if res is None:
                    return None
                if isinstance(res, DataType):
                    dtypes = res.dtypes
                else:
                    dtypes = None
            return res
        else:
            return None

    def _clean_key(self, key):
        if isinstance(key, list):
            return [self._clean_key(item) for item in key]
        elif not isinstance(key, tuple):
            return (key,)
        return key
        
    def __getitem__(self, key):
        key = self._clean_key(key)
        res = self.df[key]
        t = self._get_columns(key)
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
        self_keylen = len(self.df.columns[0])
        if other_keylen > self_keylen:
            pad = ("",) * (other_keylen - self_keylen)
            self.df.columns = pd.MultiIndex.from_tuples(col + pad for col in self.df.columns)
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
            dtypes = self.dtypes
            for item in key[:-1]:
                if item not in dtypes:
                    dtypes[item] = DataType(DataInstance, {})
                dtypes = dtypes[item].dtypes
            dtypes[key[-1]] = DataType(type(value), value.dtypes)
            value = value.df
        if isinstance(value, pd.DataFrame) and not isinstance(key, list):
            other_columns = [key + col for col in value.columns]
            other_columns = self._adapt_keys(other_columns)
            self.df[other_columns] = value
        else:
            if isinstance(key, list):
                key = self._adapt_keys(key)
            else:
                key = self._adapt_key(key)
            self.df[key] = value
            if not isinstance(key, list): key = [key]
            for subkey in key:
                self._save_dtype(subkey, self.df.dtypes[subkey])

    def __delitem__(self, name):
        key = self._clean_key(name)
        del self.df[name]
        if not isinstance(name, list): name = [name]
        for item in name:
            dtypes = self.dtypes
            for part in item[:-1]:
                if part not in dtypes: break
                if not isinstance(dtypes[part], DataType): break
                dtypes = dtypes[part].dtypes
            else:
                del dtypes[item[-1]]
                
            
    def wrap(self, value):
        if isinstance(value, pd.DataFrame):
            return type(self)(value, copy_dtypes(self.dtypes))
        return value
    
    def __getattr__(self, name):
        attr = getattr(self.df, name)
        if isinstance(attr, types.MethodType):
            def wrapper(*arg, **kw):
                return self.wrap(attr(*arg, **kw))
            return wrapper
        else:
            return self.wrap(attr)

    @property
    def loc(self):
        return DataInstanceLoc(self)
    
    def __repr__(self):
        t = type(self)
        return "%s.%s:\n%s" % (t.__module__, t.__name__, repr(self.df))

    def summary(self):
        res = DataInstance(self.df.copy(), copy_dtypes(self.dtypes))
        for name, dtype in self.dtypes.items():
            del res[name]
            if isinstance(dtype, DataType):
                if dtype.cls is None:
                    pass
                else:
                    res[(name, "[%s]" % (dtype.cls.__name__,))] = self[name].summary()
            else:
                res[(name, "[%s]" % (dtype,))] = self[name]
        return res
