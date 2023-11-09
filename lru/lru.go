package lru

import "container/list"

// Cache is a LRU cache. It is not safe for concurrent access.
type Cache struct {
	maxBytes  int64 // 允许使用的最大内存，0 表示无限制
	nbytes    int64 // 当前已使用的内存
	ll        *list.List
	cache     map[string]*list.Element
	OnEvicted func(key string, value Value) // 某条记录被移除时的回调函数，可以为 nil
}

type entry struct {
	key   string
	value Value
}

// Value use Len to count how many bytes it takes
type Value interface {
	Len() int
}

// New is the constructor of Cache
func New(maxBytes int64, onEvicted func(string, Value)) *Cache {
	return &Cache{
		maxBytes:  maxBytes,
		ll:        list.New(),
		cache:     make(map[string]*list.Element),
		OnEvicted: onEvicted,
	}
}

// Get look up a key's value
func (c *Cache) Get(key string) (value Value, ok bool) {
	// 从字典中找到对应的双向链表的节点
	if ele, ok := c.cache[key]; ok {
		// 将该节点移动到队头
		c.ll.MoveToFront(ele)
		// 取出节点的值
		kv := ele.Value.(*entry)
		return kv.value, true
	}
	return
}

// RemoveOldest removes the oldest item
func (c *Cache) RemoveOldest() {
	ele := c.ll.Back()
	if ele != nil {
		c.ll.Remove(ele)
		kv := ele.Value.(*entry)
		delete(c.cache, kv.key)
		c.nbytes -= int64(len(kv.key)) + int64(kv.value.Len())
		if c.OnEvicted != nil {
			c.OnEvicted(kv.key, kv.value)
		}
	}
}

// Add adds a value to the cache
func (c *Cache) Add(key string, value Value) {
	// 如果键存在，则更新对应节点的值，并将该节点移到队头
	if ele, ok := c.cache[key]; ok {
		c.ll.MoveToFront(ele)
		kv := ele.Value.(*entry)
		// 更新节点的值
		c.nbytes += int64(value.Len()) - int64(kv.value.Len())
		kv.value = value
	} else {
		// 如果键不存在，则在队头添加新节点
		ele := c.ll.PushFront(&entry{key, value})
		c.cache[key] = ele
		c.nbytes += int64(len(key)) + int64(value.Len())
	}
	// 如果超过了设定的最大内存，则移除最少访问的节点
	for c.maxBytes != 0 && c.maxBytes < c.nbytes {
		c.RemoveOldest()
	}
}

// Len returns the number of cache entries
func (c *Cache) Len() int {
	return c.ll.Len()
}
