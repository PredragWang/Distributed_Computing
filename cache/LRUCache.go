/*
Copyright 2017 by Guanyu Wang

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
     http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/


// package cache implements different cache strategies
package cache
import "sync"

// Cache key and value types
type KeyType interface{}
type ValueType interface{}

// Cache entry data structure
type CacheEntry struct {
    key   KeyType
    value ValueType
    prev  *CacheEntry
    next  *CacheEntry
}

// LRU cache data structure
// This implementation of LRU is thread safe.
// key and value can be all types supported in Golang
type LRUCache struct {
    mutex          sync.Mutex
    cacheMap       map[KeyType]*CacheEntry
    cacheList      *CacheEntry
    cacheListEnd   *CacheEntry
    capacity       int
}

// Create a new LRUCache instance
func NewLRUInstance(capacity int) LRUCache {
    return LRUCache {
        cacheMap     : make(map[KeyType]*CacheEntry),
        cacheList    : nil,
        cacheListEnd : nil,
        capacity     : capacity,
    }
}

// Remove the least recently used CacheEntry
func (this *LRUCache) removeLRU() {
  delete(this.cacheMap, this.cacheList.key)
  this.cacheList.next.prev = nil
  this.cacheList = this.cacheList.next
}

// move a node to the end of the cache list
func (this *LRUCache) moveToEnd (node *CacheEntry) {
    if node == this.cacheListEnd {
        return
    }
    this.mutex.Lock()
    node.next.prev = node.prev
    if node == this.cacheList {
        this.cacheList = node.next
    } else {
        node.prev.next = node.next
    }
    this.cacheListEnd.next = node
    node.prev = this.cacheListEnd
    node.next = nil
    this.cacheListEnd = node
    this.mutex.Unlock()
}

// add a new cache node to the end of the cache list
func (this *LRUCache) addNew(key KeyType, value ValueType) {
    this.mutex.Lock()
    newNode := CacheEntry {
        key : key,
        value : value,
        prev : this.cacheListEnd,
        next : nil,
    }
    if this.cacheList == nil {
        this.cacheList = &newNode
    } else {
        this.cacheListEnd.next = &newNode
    }
    this.cacheListEnd = &newNode
    this.cacheMap[key] = &newNode
    if len(this.cacheMap) > this.capacity {
        this.removeLRU();
    }
    this.mutex.Unlock()
}

// Get the value by the given key
func (this *LRUCache) Get(key KeyType) (ValueType, bool) {
    node,exists := this.cacheMap[key]
    ret_value := ValueType(nil)
    if exists {
      ret_value = node.value
      this.moveToEnd(node)
    }

    return ret_value,exists
}

// Put a new (key, value) into the cache,
// Update if key exists
func (this *LRUCache) Put(key KeyType, value ValueType)  {
    if this.capacity < 1 {
        return
    }
    node,exists := this.cacheMap[key]
    if exists {
        node.value = value
        this.moveToEnd(node)
    } else {
        this.addNew(key, value)
    }
}
