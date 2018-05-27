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
import (
  "sync"
  "container/list"
)

type LFUCacheEntry struct {
    key         KeyType
    value       ValueType
    frequency   int
}

type CacheListEntry struct{
    frequency  int
    entries    *list.List
}

type LFUCache struct {
    mutex         sync.Mutex
    capacity      int
    size          int
    lists         *list.List
    cacheMap      map[KeyType]*list.Element
    frequencyMap  map[int]*list.Element
}


func NewLFUInstance(capacity int) LFUCache {
    return LFUCache {
        capacity     : capacity,
        size         : 0,
        lists        : list.New(),
        cacheMap     : make(map[KeyType]*list.Element),
        frequencyMap : make(map[int]*list.Element),
    }
}

func (this *LFUCache) removeLFU() ValueType{
    if this.lists.Len() > 0 {
        this.mutex.Lock()
        lf_ele := this.lists.Front()
        lf := lf_ele.Value.(*CacheListEntry)
        ret_ele := lf.entries.Front()
        ret_v := lf.entries.Remove(ret_ele).(*LFUCacheEntry)
        delete(this.cacheMap, ret_v.key)
        if lf.entries.Len() == 0 {
            this.lists.Remove(lf_ele)
            delete(this.frequencyMap, lf.frequency)
        }
        this.mutex.Unlock()
        return ret_v
    }
    return nil
}

// add a new cache
func (this *LFUCache) addNew(key KeyType, value ValueType) {
    this.mutex.Lock()
    if this.size == this.capacity {
        this.removeLFU()
    } else {
        this.size ++
    }

    ce := &LFUCacheEntry{
        key       : key,
        value     : value,
        frequency : 1,
    }
    cle_ele := this.lists.Front()
    var ce_ele *list.Element
    if cle_ele != nil && cle_ele.Value.(*CacheListEntry).frequency == 1 {
        ce_ele = cle_ele.Value.(*CacheListEntry).entries.PushBack(ce)
    } else {
        cle := &CacheListEntry{
            frequency : 1,
            entries   : list.New(),
        }
        ce_ele = cle.entries.PushBack(ce)
        cle_ele := this.lists.PushFront(cle)
        this.frequencyMap[1] = cle_ele
    }
    this.cacheMap[key] = ce_ele
    this.mutex.Unlock()
}

// increment the frequency of a cache
func (this *LFUCache) incFreq(cle_ele *list.Element, ce_ele *list.Element) {
  this.mutex.Lock()
    cle, ce := cle_ele.Value.(*CacheListEntry), ce_ele.Value.(*LFUCacheEntry)
    // remove the cache entry from the frequency list
    cle.entries.Remove(ce_ele)
    ce.frequency ++
    if cle_ele.Next() != nil && cle_ele.Next().Value.(*CacheListEntry).frequency == ce.frequency{
        ce_ele = cle_ele.Next().Value.(*CacheListEntry).entries.PushBack(ce)
    } else {
        new_cle := &CacheListEntry{
            frequency : ce.frequency,
            entries   : list.New(),
        }
        ce_ele = new_cle.entries.PushBack(ce)
        new_cle_ele := this.lists.InsertAfter(new_cle, cle_ele)
        this.frequencyMap[ce.frequency] = new_cle_ele
    }
    this.cacheMap[ce.key] = ce_ele

    if cle.entries.Len() == 0 {
        this.lists.Remove(cle_ele)
        delete(this.frequencyMap, ce.frequency-1)
    }
    this.mutex.Unlock()
}


func (this *LFUCache) Get(key KeyType) ValueType {
    if this.capacity <= 0 {
        return ValueType(nil)
    }
    ce_ele,exists := this.cacheMap[key]
    if !exists {
        return ValueType(nil)
    }
    ce := ce_ele.Value.(*LFUCacheEntry)
    cle_ele,_ := this.frequencyMap[ce.frequency]
    if cle_ele != nil {
        go this.incFreq(cle_ele, ce_ele)
    }
    return ce.value
}


func (this *LFUCache) Put(key KeyType, value ValueType) bool {
    if this.capacity <= 0 {
        return false
    }
    ce_ele,exists := this.cacheMap[key]
    if exists {
        ce_ele.Value.(*LFUCacheEntry).value = value
        cle_ele,_ := this.frequencyMap[ce_ele.Value.(*LFUCacheEntry).frequency]
        if cle_ele != nil {
            go this.incFreq(cle_ele, ce_ele)
        }
    } else {
        go this.addNew(key, value)
    }
    return true
}
