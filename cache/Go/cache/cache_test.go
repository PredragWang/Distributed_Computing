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
  "testing"
)

var lru_tests = []struct {
  key        KeyType
  value      ValueType
  expected   []ValueType
} {
{1, 1, []ValueType{1, nil}},
{2, 2, []ValueType{2, nil}},
{1, nil, []ValueType{1, nil}},
{3, 3, []ValueType{3, nil}},
{2, nil, []ValueType{2, nil}},
{3, nil, []ValueType{3, nil}},
{4, 4, []ValueType{4, nil}},
{1, nil, []ValueType{1, nil}},
{3, nil, []ValueType{3, nil}},
{4, nil, []ValueType{4, nil}},
}

var lfu_tests = []struct {
  key        KeyType
  value      ValueType
  expected   []ValueType
} {
  {1, 1, []ValueType{1, nil}},
  {2, 2, []ValueType{2, nil}},
  {1, nil, []ValueType{1, nil}},
  {3, 3, []ValueType{3, nil}},
  {2, nil, []ValueType{2, nil}},
  {3, nil, []ValueType{3, nil}},
  {4, 4, []ValueType{4, nil}},
  {1, nil, []ValueType{1, nil}},
  {3, nil, []ValueType{3, nil}},
  {4, nil, []ValueType{4, nil}},
}

func TestLRU(t *testing.T) {
  lru := NewLRUInstance(3)
	for _, tt := range lru_tests {
    if tt.value != nil {
      lru.Put(tt.key, tt.value)
    } else {
      got_v,_ := lru.Get(tt.key)
      if got_v  != tt.expected[0] && got_v != tt.expected[1] {
  		  t.Fatalf("For key %v get %v; want %v", tt.key, got_v, tt.expected )
      }
    }
	}
}

func TestLFU(t *testing.T) {
  lfu := NewLFUInstance(3)
	for _, tt := range lfu_tests {
    if tt.value != nil {
      lfu.Put(tt.key, tt.value)
    } else {
      got_v := lfu.Get(tt.key)
      if got_v  != tt.expected[0] && got_v != tt.expected[1] {
  		  t.Fatalf("For key %v get %v; want %v", tt.key, got_v, tt.expected )
      }
    }
	}
}
