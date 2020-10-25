class SpecialNumeric(object):
    def __add__(self, *arg, **kw): return self.__getattr__("__add__")(*arg, **kw)
    def __sub__(self, *arg, **kw): return self.__getattr__("__sub__")(*arg, **kw)
    def __mul__(self, *arg, **kw): return self.__getattr__("__mul__")(*arg, **kw)
    def __floordiv__(self, *arg, **kw): return self.__getattr__("__floordiv__")(*arg, **kw)
    def __mod__(self, *arg, **kw): return self.__getattr__("__mod__")(*arg, **kw)
    def __divmod__(self, *arg, **kw): return self.__getattr__("__divmod__")(*arg, **kw)
    def __pow__(self, *arg, **kw): return self.__getattr__("__pow__")(*arg, **kw)
    def __lshift__(self, *arg, **kw): return self.__getattr__("__lshift__")(*arg, **kw)
    def __rshift__(self, *arg, **kw): return self.__getattr__("__rshift__")(*arg, **kw)
    def __and__(self, *arg, **kw): return self.__getattr__("__and__")(*arg, **kw)
    def __xor__(self, *arg, **kw): return self.__getattr__("__xor__")(*arg, **kw)
    def __or__(self, *arg, **kw): return self.__getattr__("__or__")(*arg, **kw)
    def __div__(self, *arg, **kw): return self.__getattr__("__div__")(*arg, **kw)
    def __truediv__(self, *arg, **kw): return self.__getattr__("__truediv__")(*arg, **kw)
    def __radd__(self, *arg, **kw): return self.__getattr__("__radd__")(*arg, **kw)
    def __rsub__(self, *arg, **kw): return self.__getattr__("__rsub__")(*arg, **kw)
    def __rmul__(self, *arg, **kw): return self.__getattr__("__rmul__")(*arg, **kw)
    def __rdiv__(self, *arg, **kw): return self.__getattr__("__rdiv__")(*arg, **kw)
    def __rtruediv__(self, *arg, **kw): return self.__getattr__("__rtruediv__")(*arg, **kw)
    def __rfloordiv__(self, *arg, **kw): return self.__getattr__("__rfloordiv__")(*arg, **kw)
    def __rmod__(self, *arg, **kw): return self.__getattr__("__rmod__")(*arg, **kw)
    def __rdivmod__(self, *arg, **kw): return self.__getattr__("__rdivmod__")(*arg, **kw)
    def __rpow__(self, *arg, **kw): return self.__getattr__("__rpow__")(*arg, **kw)
    def __rlshift__(self, *arg, **kw): return self.__getattr__("__rlshift__")(*arg, **kw)
    def __rrshift__(self, *arg, **kw): return self.__getattr__("__rrshift__")(*arg, **kw)
    def __rand__(self, *arg, **kw): return self.__getattr__("__rand__")(*arg, **kw)
    def __rxor__(self, *arg, **kw): return self.__getattr__("__rxor__")(*arg, **kw)
    def __ror__(self, *arg, **kw): return self.__getattr__("__ror__")(*arg, **kw)
    def __iadd__(self, *arg, **kw): return self.__getattr__("__iadd__")(*arg, **kw)
    def __isub__(self, *arg, **kw): return self.__getattr__("__isub__")(*arg, **kw)
    def __imul__(self, *arg, **kw): return self.__getattr__("__imul__")(*arg, **kw)
    def __idiv__(self, *arg, **kw): return self.__getattr__("__idiv__")(*arg, **kw)
    def __itruediv__(self, *arg, **kw): return self.__getattr__("__itruediv__")(*arg, **kw)
    def __ifloordiv__(self, *arg, **kw): return self.__getattr__("__ifloordiv__")(*arg, **kw)
    def __imod__(self, *arg, **kw): return self.__getattr__("__imod__")(*arg, **kw)
    def __ipow__(self, *arg, **kw): return self.__getattr__("__ipow__")(*arg, **kw)
    def __ilshift__(self, *arg, **kw): return self.__getattr__("__ilshift__")(*arg, **kw)
    def __irshift__(self, *arg, **kw): return self.__getattr__("__irshift__")(*arg, **kw)
    def __iand__(self, *arg, **kw): return self.__getattr__("__iand__")(*arg, **kw)
    def __ixor__(self, *arg, **kw): return self.__getattr__("__ixor__")(*arg, **kw)
    def __ior__(self, *arg, **kw): return self.__getattr__("__ior__")(*arg, **kw)
    def __neg__(self, *arg, **kw): return self.__getattr__("__neg__")(*arg, **kw)
    def __pos__(self, *arg, **kw): return self.__getattr__("__pos__")(*arg, **kw)
    def __abs__(self, *arg, **kw): return self.__getattr__("__abs__")(*arg, **kw)
    def __invert__(self, *arg, **kw): return self.__getattr__("__invert__")(*arg, **kw)
    def __complex__(self, *arg, **kw): return self.__getattr__("__complex__")(*arg, **kw)
    def __int__(self, *arg, **kw): return self.__getattr__("__int__")(*arg, **kw)
    def __long__(self, *arg, **kw): return self.__getattr__("__long__")(*arg, **kw)
    def __float__(self, *arg, **kw): return self.__getattr__("__float__")(*arg, **kw)
    def __oct__(self, *arg, **kw): return self.__getattr__("__oct__")(*arg, **kw)
    def __hex__(self, *arg, **kw): return self.__getattr__("__hex__")(*arg, **kw)
    def __index__(self, *arg, **kw): return self.__getattr__("__index__")(*arg, **kw)
    def __coerce__(self, *arg, **kw): return self.__getattr__("__coerce__")(*arg, **kw)
