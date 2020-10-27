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

class DataSetInstance(object):
    def __init__(self, instance, *tags):
        self.id = id(instance)
        self.instance = instance
        self.tags = set(tags)

    @property
    def all_tags(self):
        return set.union(self.tags, inspect.getmro(type(self.instance)))
        
    def __repr__(self):
        res = valuerepr(self.instance)
        if self.tags:
            res += " / " + ",".join(str(tag) for tag in self.tags)
        return res
    
class Storage(object):
    def __init__(self):
        self.by_tag = {}
        self.datasets = {}
        self.handlers = {}

    def on(self, handler, *tags):
        self.handlers[frozenset(tags)] = handler

    def trigger(self, *tags, **kw):
        for handler_tags, handler in self.handlers.items():
            if len(handler_tags - set(tags)) == 0:
                handler(*tags, **kw)
        
    def add(self, instance, *tags):
        tags = [tagify(tag) for tag in tags]
        
        instance = DataSetInstance(instance, *tags)
        self.datasets[instance.id] = instance

        for tag in instance.all_tags:
            if tag not in self.by_tag:
                self.by_tag[tag] = weakref.WeakSet()
            self.by_tag[tag].add(instance)

        self.trigger(action="add", instance=instance.instance, *instance.all_tags)
            
    def query(self, qp):
        if not qp:
            return set(self.datasets.values())
        qs = [set((self.datasets[id(t)],))
              if id(t) in self.datasets
              else set(self.by_tag.get(tagify(t), ()))
              for t in qp]
        if not qs:
            return set()
        return {instance for instance in set.intersection(*qs)}
            
    def instance_query(self, qp):
        return {instance.instance for instance in self.query(qp)}

    def remove(self, qp):
        for instance in self.query(qp):
            del self.dataset[instance.id]
        self.trigger(action="remove", instance=instance.instance, *qp)
            
    def untag(self, qp, *tags):
        for old_instance in self.query(qp):
            instance = DataSetInstance(
                old_instance.instance, *(old_instance.all_tags - set(tags)))
            self.datasets[instance.id] = instance
            for tag in instance.all_tags:
                self.by_tag[tag].add(instance)
            self.trigger(action="untag", instance=instance.instance, tags=tags, *old_instance.all_tags)

    def tag(self, qp, *tags):
        for tag in tags:
            if tag not in self.by_tag:
                self.by_tag[tag] = weakref.WeakSet()
        for old_instance in self.query(qp):
            instance = DataSetInstance(
                old_instance.instance, *(set.union(old_instance.tags, tags)))
            self.datasets[instance.id] = instance
            for tag in instance.all_tags:
                self.by_tag[tag].add(instance)
            self.trigger(action="tag", instance=instance.instance, tags=tags, *instance.all_tags)
    
class DataSet(object):
    def __new__(cls, items = []):
        self = cls.new_from_storage_and_filter()
        for instance, tags in items:
            self.storage.add(instance, *tags)
        return self
    
    @classmethod
    def new_from_storage_and_filter(cls, storage = None, filter=None):
        self = object.__new__(cls)
        self.storage = storage if storage is not None else Storage()
        self.filter = filter if filter is not None else set()
        return self
        
    def on(self, handler):
        self.storage.on(handler, *self.filter)

    def trigger(self, **kw):
        self.storage.trigger(*self.filter, **kw)

    @property
    def tags(self):
        res = self.storage.query(self.filter)
        if not res: return set()
        return set.union(*(instance.tags for instance in res))
        
    def __call__(self, *arg):
        return self.__getitem__(arg)
        
    def __getitem__(self, qp):
        return type(self).new_from_storage_and_filter(
            self.storage, set.union(self.filter, to_tagset(qp)))

    def __setitem__(self, key, value):
        # Make foo["tag1", "tag2"...] += "Ntag" work
        if isinstance(value, DataSet) and id(value.storage) == id(self.storage):
            return
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
        return len(self[qp]) > 0
    
    def __iter__(self):
        return iter(self.storage.instance_query(self.filter))

    def __eq__(self, other):
        return set(self) == set(other)
    
    def __iadd__(self, tags):
        self.storage.tag(self.filter, *to_tagset(tags))
        return self

    def __isub__(self, tags):
        self.storage.untag(self.filter, *to_tagset(tags))
        return self

    def items(self):
        return ((instance.instance, instance.tags) for instance in self.storage.query(self.filter))
