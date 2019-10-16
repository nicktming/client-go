/*
Copyright 2015 The Kubernetes Authors.

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

package workqueue

import (
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/util/clock"
)

// Interface的接口设计原则
// 与一般的queue有两点区别:
// 1. 增加了Done方法, 告诉queue对这个item的处理已经结束了
// 2. 以往的queue在pop之后就对这个item的状态不管了, 但是在该iterface中明显需要进行管理,
//    因为会通过调用Done方法 所以需要知道哪些item正在处理


type Interface interface {
	Add(item interface{})
	Len() int
	Get() (item interface{}, shutdown bool)
	Done(item interface{})
	ShutDown()
	ShuttingDown() bool
}

// New constructs a new work queue (see the package comment).

func New() *Type {
	return NewNamed("")
}

func NewNamed(name string) *Type {
	rc := clock.RealClock{}
	return newQueue(
		rc,
		globalMetricsFactory.newQueueMetrics(name, rc),
		defaultUnfinishedWorkUpdatePeriod,
	)
}

func newQueue(c clock.Clock, metrics queueMetrics, updatePeriod time.Duration) *Type {
	t := &Type{
		clock:                      c,
		dirty:                      set{},
		processing:                 set{},
		cond:                       sync.NewCond(&sync.Mutex{}),
		metrics:                    metrics,
		unfinishedWorkUpdatePeriod: updatePeriod,
	}
	go t.updateUnfinishedWorkLoop()
	return t
}

const defaultUnfinishedWorkUpdatePeriod = 500 * time.Millisecond

// Type is a work queue (see the package comment).

// Type 这个queue的设计:
// 1. 保存了需要被处理的item
// 2. 保存了正在被处理的item
// 3. 保证不能同时有两个相同的item被处理

// 所以如果一个item正在被处理, 此时又add了一个相同的item, 那不能加入到queue中(把该item暂时加到了dirty)
// 因为如果在该item没有Done之前出队列了, 就会有两个相同的item在被处理

// 这种情况的解决办法就是 把该item暂时加到dirty中, 得到其处理结束后如果dirty中有就再加回到queue中.
type Type struct {
	// queue defines the order in which we will work on items. Every
	// element of queue should be in the dirty set and not in the
	// processing set.

	// queue定义了需要处理的item的顺序
	// 这些element应该出现在dirty中 而不会在processing中.
	queue []t

	// dirty defines all of the items that need to be processed.
	// dirty保存着那些需要被处理的item
	dirty set

	// Things that are currently being processed are in the processing set.
	// These things may be simultaneously in the dirty set. When we finish
	// processing something and remove it from this set, we'll check if
	// it's in the dirty set, and if so, add it to the queue.

	// 代表那些正在被处理的元素,
	processing set

	cond *sync.Cond

	shuttingDown bool

	metrics queueMetrics

	unfinishedWorkUpdatePeriod time.Duration
	clock                      clock.Clock
}

type empty struct{}
type t interface{}
type set map[t]empty

func (s set) has(item t) bool {
	_, exists := s[item]
	return exists
}

func (s set) insert(item t) {
	s[item] = empty{}
}

func (s set) delete(item t) {
	delete(s, item)
}

// Add marks item as needing processing.
func (q *Type) Add(item interface{}) {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()
	// 如果queue已经关闭了
	if q.shuttingDown {
		return
	}
	// 如果已经在queue中了 就直接返回了
	if q.dirty.has(item) {
		return
	}

	q.metrics.add(item)
	// 加入到dirty中
	q.dirty.insert(item)
	// 如果该元素正在被处理, 那直接返回了 不会加入到queue中
	if q.processing.has(item) {
		return
	}

	q.queue = append(q.queue, item)
	q.cond.Signal()
}

// Len returns the current queue length, for informational purposes only. You
// shouldn't e.g. gate a call to Add() or Get() on Len() being a particular
// value, that can't be synchronized properly.

// 返回queue中长度
// 也就是目前等待处理的item的个数
func (q *Type) Len() int {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()
	return len(q.queue)
}

// Get blocks until it can return an item to be processed. If shutdown = true,
// the caller should end their goroutine. You must call Done with item when you
// have finished processing it.

// 1. 如果queue没有关闭 但是目前没有元素 一直waiting
// 2. 如果queue已经关闭 则返回nil, true
// 3. 获得队列的头item 并且更新queue数组
// 4. 将该item加入到processing中 表明该item正在被处理
// 5. 从dirty和queue中删除该item
// 6. 返回该item, false
func (q *Type) Get() (item interface{}, shutdown bool) {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()
	for len(q.queue) == 0 && !q.shuttingDown {
		q.cond.Wait()
	}
	if len(q.queue) == 0 {
		// We must be shutting down.
		return nil, true
	}

	// 获得队列的头 并且更新queue数组
	item, q.queue = q.queue[0], q.queue[1:]

	q.metrics.get(item)

	// 1. 将该item加入到processing中, 表明该item正在被处理
	// 2. 将该item从dirty中删除
	q.processing.insert(item)
	q.dirty.delete(item)

	return item, false
}

// Done marks item as done processing, and if it has been marked as dirty again
// while it was being processed, it will be re-added to the queue for
// re-processing.

// 表明该item已经结束了
func (q *Type) Done(item interface{}) {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()

	q.metrics.done(item)

	// 如果结束了 就从processing中删除
	q.processing.delete(item)
	// 如果dirty中有 在把item加回去
	// 这里主要是因为在处理item的过程中, 上流程序又调用了Add方法
	// 因为该item正在被处理, 此时如果加入到queue中, 然后又Get拿到该item
	// 此时就会有两个同样的item正在被处理

	// 所以最终设计的目的是为了保证正在被处理的过程中保证每个都不一样 不会出现有两个相同的item正在被处理
	if q.dirty.has(item) {
		q.queue = append(q.queue, item)
		q.cond.Signal()
	}
}

// ShutDown will cause q to ignore all new items added to it. As soon as the
// worker goroutines have drained the existing items in the queue, they will be
// instructed to exit.

// 关闭queue, 并广播
func (q *Type) ShutDown() {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()
	q.shuttingDown = true
	q.cond.Broadcast()
}

// 返回是否已经关闭了queue
func (q *Type) ShuttingDown() bool {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()

	return q.shuttingDown
}

// 定时更新metrics
func (q *Type) updateUnfinishedWorkLoop() {
	t := q.clock.NewTicker(q.unfinishedWorkUpdatePeriod)
	defer t.Stop()
	for range t.C() {
		if !func() bool {
			q.cond.L.Lock()
			defer q.cond.L.Unlock()
			if !q.shuttingDown {
				q.metrics.updateUnfinishedWork()
				return true
			}
			return false

		}() {
			return
		}
	}
}
