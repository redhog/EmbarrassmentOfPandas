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

def to_tagset(key):
    if isinstance(key, slice) and key.start is None and key.stop is None and key.step is None:
        key = ()
    if key is None:
        key = ()
    if not isinstance(key, (list, tuple, set)):
        key = (key),
    return set(tagify(item) for item in key)

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

    def query(self, qp):
        if not qp:
            return set(self.datasets.values())
        qs = [set(self.by_tag.get(tagify(t), ()))
              for t in qp]
        if not qs:
            return set()
        return {instance for instance in set.intersection(*qs)}
            
    def instance_query(self, qp):
        return {instance.instance for instance in self.query(qp)}

    def remove(self, qp):
        for instance in self.query(qp):
            del self.dataset[instance.id]

    def untag(self, qp, *tags):
        for instance in self.query(qp):
            self.dataset[instance.id] = DataSetInstance(
                instance.instance, *(instance.tags - set(tags)))

    def tag(self, qp, *tags):
        for tag in tags:
            if tag not in self.by_tag:
                self.by_tag[tag] = weakref.WeakSet()
        for instance in self.query(qp):
            self.dataset[instance.id] = DataSetInstance(
                instance.instance, *(set.union(instance.tags, tags)))
            for tag in tags:
                self.by_tag[tag].add(instance)
    
class DataSet(object):
    def __init__(self, storage = None, filter=None):
        self.storage = storage if storage is not None else Storage()
        self.filter = filter if filter is not None else set()
        
    def __getitem__(self, qp):
        if id(qp) in self.storage.datasets:
            if len(self.filter - self.storage.datasets[id(qp)].tags) > 0:
                raise KeyError(qp)
            return self.storage.datasets[id(qp)].tags - self.filter
        return DataSet(self.storage, set.union(self.filter, to_tagset(qp)))

    def __setitem__(self, key, value):
        self.storage.add(value, *set.union(self.filter, to_tagset(key)))

    def __delitem__(self, key):
        self.storage.remove(
            set.union(self.filter, set.union(self.filter, to_tagset(key))))

    def __repr__(self):
        return "\n".join(repr(instance)
                         for instance in self.storage.query(self.filter))

    def __len__(self):
        return len(self.storage.instance_query(self.filter))

    def __contains__(self, qp):
        if id(qp) in self.storage.datasets:
            return len(self.filter - self.storage.datasets[id(qp)].tags) == 0
        return len(self[qp]) > 0
    
    def __iter__(self):
        return iter(self.storage.instance_query(self.filter))

    def __eq__(self, other):
        return set(self) == set(other)
    
    def __iadd__(self, tags):
        self.storage.tag(self.filter, *to_tagset(tags))

    def __isub__(self, tags):
        self.storage.untag(self.filter, *to_tagset(tags))
