import uuid
import weakref
import inspect

def valuerepr(obj):
    s = str(obj)
    if "\n" not in s:
        return s[:30]
    t = type(obj)
    t = "%s.%s" % (t.__module__, t.__name__)
    try:
        return "<%s#%s>" % (t, hash(obj))
    except:
        return "<%s@%s>" % (t, id(obj))

def tagify(tag):
    if isinstance(tag, dict):
        return Tag(tag)
    if isinstance(tag, slice):
        return Tag({tag.start: tag.stop})
    return tag
    
class DataSetInstance(object):
    def __init__(self, instance, *tags):
        self.id = id(instance)
        self.instance = instance
        self.tags = set(tags)

    def __repr__(self):
        res = valuerepr(self.instance)
        if self.tags:
            res += " / " + ",".join(str(tag) for tag in self.tags)
        return res
        
class Tag(object):
    def __init__(self, *arg, **attrs):
        if arg:
            attrs = arg[0]
        self.attrs = attrs
    def __repr__(self):
        return "[%s]" % ",".join("%s:%s" % (key, valuerepr(self.attrs[key])) for key in sorted(self.attrs.keys()))
    def __hash__(self):
        def hashorid(obj):
            try:
                return hash(obj)
            except:
                return id(obj)
        return hash(",".join("%s:%s" % (key, hashorid(self.attrs[key])) for key in sorted(self.attrs.keys())))
    def __eq__(self, other):
        return repr(self) == repr(other)
    def __getitem__(self, key):
        return self.attrs[key]
    
class Storage(object):
    def __init__(self):
        self.by_tag = {}
        self.datasets = {}

    def add(self, instance, *tags):
        itype = type(instance)

        tags = [tagify(tag) for tag in tags]
        
        instance = DataSetInstance(instance, *tags)
        self.datasets[instance.id] = instance

        for tag in tuple(tags) + inspect.getmro(itype):
            if tag not in self.by_tag:
                self.by_tag[tag] = weakref.WeakSet()
            self.by_tag[tag].add(instance)

    def remove(self, qp):
        for t in qp:
            if id(t) in self.datasets:
                del self.datasets[id(t)]
            else:
                t = tagify(t)
                for instance in self.by_tag[t]:
                    instance.tags.remove(t)
                del self.by_tag[t]

    def query(self, qp):
        if id(qp) in self.datasets:
            return self.datasets[id(qp)].tags
        else:
            if not isinstance(qp, tuple): qp = (qp,)
            qs = [set(self.by_tag.get(tagify(t), ()))
                  for t in qp]
            if not qs:
                return set()
            return {instance.instance for instance in set.intersection(*qs)}

class DataSet(object):
    def __init__(self, storage = None):
        self.storage = storage if storage is not None else Storage()
        
    def __getitem__(self, qp):
        return self.storage.query(qp)

    def __contains__(self, qp):
        if id(qp) in self.storage.datasets: return True
        return len(self[qp]) > 0
    
    def __setitem__(self, key, value):
        if isinstance(key, slice) and key.start is None and key.stop is None and key.step is None:
            key = ()
        if key is None:
            key = ()
        if not isinstance(key, tuple): key = (key,)
        self.storage.add(value, *key)

    def __delitem__(self, key):
        if not isinstance(key, tuple): key = (key,)
        self.storage.remove(key)

    def __repr__(self):
        return "\n".join(repr(instance) for instance in self.storage.datasets.values())
