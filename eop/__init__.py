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

def copy_dtypes(dtypes):
    if isinstance(dtypes, dict):
        return {key:copy_dtypes(value) for key, value in dtypes.items()}
    elif isinstance(dtypes, tuple):
        return (dtypes[0], copy_dtypes(dtypes[1]))
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
        
    def _enforce_dtypes(self, prefix = (), dtypes = None):
        if dtypes is None: dtypes = self.dtypes
        for key, value in dtypes.items():
            if isinstance(value, tuple):
                self._enforce_dtypes(prefix + (key,), value[1])
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
                if isinstance(res, tuple):
                    dtypes = res[1]
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
        if isinstance(t, (dict, tuple)) and isinstance(res, pd.Series):
            res = pd.DataFrame({key: res}).T
        if isinstance(t, dict):
            return type(self)(res, t)
        elif isinstance(t, tuple):
            return t[0](res, t[1])
        else:
            return res

    def __setitem__(self, key, value):
        key = self._clean_key(key)
        if isinstance(value, DataInstance):
            other_columns = [key + col for col in value.df.columns]
            other_keylen = len(other_columns[0])
            self_keylen = len(self.df.columns[0])
            if other_keylen > self_keylen:
                pad = (None,) * (other_keylen - self_keylen)
                self.df.columns = pd.MultiIndex.from_tuples(key + pad for key in self.df.columns)
            elif self_keylen > other_keylen:
                pad = (None,) * (self_keylen - other_keylen)
                other_columns = [key + pad for key in other_columns]
            self.df[other_columns] = value.df
            if not isinstance(key, tuple): key = (key,)
            dtypes = self.dtypes
            for item in key[:-1]:
                if item not in dtypes:
                    dtypes[item] = (DataInstance, {})
            dtypes[key[-1]] = (type(value), value.dtypes)
        else:
            self.df[key] = value

    def __delitem__(self, name):
        key = self._clean_key(name)
        del self.df[name]
        if not isinstance(name, list): name = [name]
        for item in name:
            dtypes = self.dtypes
            for part in item[:-1]:
                if part not in dtypes: break
                if not isinstance(dtypes[part], tuple): break
                dtypes = dtypes[part][1]
            else:
                del dtypes[item[-1]]
                
            
    def wrap(self, value):
        if isinstance(value, pd.DataFrame):
            return type(self)(value)
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
            if isinstance(dtype, tuple):
                del res[name]
                res[name] = self[name].summary()
        return res
        
                
class A(DataInstance): pass

class B(DataInstance): pass

class Point3d(DataInstance):
    dtypes = {"x": np.dtype("float64"),
              "y": np.dtype("float64"),
              "z": np.dtype("float64")}

    def summary(self):
        return DataInstance(pd.DataFrame({"summary": (["/".join(str(item) for item in row) for idx, row in self.df.iterrows()])}))
            
class X(DataInstance):
    dtypes = {"doi": np.dtype("int64")}
