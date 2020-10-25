# EmbarrassmentOfPandas

An embarrassment of pandas is a group of pandas. This library helps
grouping Pandas DataFrames together and provide an object oriented
programming paradigm on top of Pandas DataFrames.

EOP allows you to define new datatypes for your columns, that in turn
have columns. For example, you could define a datatype Point that has
three columns, x,y,z, and then use this datatype wherever you need to
store points in other datasets.

The data structure corresponding to a DataFrame in EOP is called an
DataInstance. The main difference is that you can subclass an
DataInstance and use that as a datatype for a column in another
DataInstance:

    >>> import pandas as pd, numpy as np, eop
    >>> class A(eop.DataInstance): pass
    ... 
    >>> class B(eop.DataInstance): pass
    ... 
    >>> a = A({"foo": [1,2,3,4], "fie":[5,6,7,8], "naja":[9,10,11,12], "hehe":[13,14,15,16]})
    >>> b = B({"fie":[25,26,27,28], "muae":[29,210,211,212]})
    >>> a["b"] = b
    >>> a.dtypes
    foo                    int64
    fie                    int64
    naja                   int64
    hehe                   int64
    b       <class '__main__.B'>
    Name: 0, dtype: object
    >>> a
      <__main__.A>                                
               foo fie naja hehe            b     
                                 <__main__.B>     
                                          fie muae
    0            1   5    9   13           25   29
    1            2   6   10   14           26  210
    2            3   7   11   15           27  211
    3            4   8   12   16           28  212
    >>> a["b"]
      <__main__.B>     
               fie muae
    0           25   29
    1           26  210
    2           27  211
    3           28  212

The main advantage of using DataInstance subclasses as datatypes is
that you can define additional methods on them. These methods would
then use regular numpy/pandas methods to operate across all the rows
of the DataInstance. For example, you could define methods like these

    >>> cloud["point"].rotate_around(coord)
    >>> ground["topo"].reproject(new_crs)
