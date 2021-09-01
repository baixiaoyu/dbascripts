package main

import (
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/openark/golib/log"
)

type Queue struct {
	sync.Mutex

	name         string
	done         chan struct{}
	queue        chan string
	queuedKeys   map[string]time.Time
	consumedKeys map[string]time.Time
}

var testQueue map[string](*Queue)
var dcLock sync.Mutex

func init() {
	testQueue = make(map[string](*Queue))
}

func StopMonitoring() {
	for _, q := range testQueue {
		q.stopMonitoring()
	}
}

func CreateOrReturnQueue(name string) *Queue {
	dcLock.Lock()
	defer dcLock.Unlock()
	if q, found := testQueue[name]; found {
		return q
	}

	q := &Queue{
		name:         name,
		queuedKeys:   make(map[string]time.Time),
		consumedKeys: make(map[string]time.Time),
		queue:        make(chan string, 1000000),
		done:         make(chan struct{}),
	}
	go q.startMonitoring()

	testQueue[name] = q

	return q
}

var Counter int

func (q *Queue) startMonitoring() {
	fmt.Printf("Queue.startMonitoring(%s)", q.name)
	ticker := time.NewTicker(time.Second) // hard-coded at every second

	for {
		select {
		case <-ticker.C: // do the periodic expiry
			fmt.Printf("queue len is " + strconv.Itoa(q.QueueLen()))
			Counter = q.QueueLen()

		case <-q.done:
			return
		}
	}
}

func (q *Queue) stopMonitoring() {
	q.done <- struct{}{}
}

func (q *Queue) QueueLen() int {
	q.Lock()
	defer q.Unlock()

	return len(q.queue) + len(q.queuedKeys)
}

func (q *Queue) Push(key string) {
	q.Lock()
	defer q.Unlock()

	// is it enqueued already?
	if _, found := q.queuedKeys[key]; found {
		return
	}

	// is it being processed now?
	if _, found := q.consumedKeys[key]; found {
		return
	}

	q.queuedKeys[key] = time.Now()
	q.queue <- key
}

func (q *Queue) Consume() string {
	q.Lock()
	queue := q.queue
	q.Unlock()

	key := <-queue

	q.Lock()
	defer q.Unlock()

	// alarm if have been waiting for too long
	timeOnQueue := time.Since(q.queuedKeys[key])
	if timeOnQueue > time.Duration(200)*time.Second {
		log.Warningf("key %v spent %.4fs waiting on a testQueue", key, timeOnQueue.Seconds())
	}

	q.consumedKeys[key] = q.queuedKeys[key]

	delete(q.queuedKeys, key)

	return key
}

func (q *Queue) Release(key string) {
	q.Lock()
	defer q.Unlock()

	delete(q.consumedKeys, key)
}

func SubtrDemo(a []string, b []string) []string {

	var c []string
	temp := map[string]struct{}{} // map[string]struct{}{}创建了一个key类型为String值类型为空struct的map，Equal -> make(map[string]struct{})

	for _, val := range b {
		if _, ok := temp[val]; !ok {
			temp[val] = struct{}{} // 空struct 不占内存空间
		}
	}

	for _, val := range a {
		if _, ok := temp[val]; !ok {
			c = append(c, val)
		}
	}

	return c
}

func Intersection(a []string, b []string) (inter []string) {
	m := make(map[string]string)
	nn := make([]string, 0)
	for _, v := range a {
		m[v] = v
	}
	for _, v := range b {
		times, _ := m[v]
		if len(times) > 0 {
			nn = append(nn, v)
		}
	}
	return nn
}

var DiscoveryQueue *Queue

func HandleDiscoveryRequests() {
	DiscoveryQueue = CreateOrReturnQueue("DEFAULT")

	// create a pool of discovery workers
	for i := uint(0); i < 50; i++ {
		go func() {
			for {
				instanceKey := DiscoveryQueue.Consume()
				DealSql(instanceKey)
				DiscoveryQueue.Release(instanceKey)
			}
		}()
	}
}
