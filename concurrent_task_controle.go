package main

import (
	"strconv"
	"sync"
	"time"

	"github.com/openark/golib/log"
)

var discoveryQueue *Queue

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
		queue:        make(chan string, 100000),
	}
	go q.startMonitoring()

	testQueue[name] = q

	return q
}

func (q *Queue) startMonitoring() {
	log.Debugf("Queue.startMonitoring(%s)", q.name)
	ticker := time.NewTicker(time.Second) // hard-coded at every second

	for {
		select {
		case <-ticker.C: // do the periodic expiry
			log.Debugf("queue len is " + strconv.Itoa(q.QueueLen()))
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
	log.Debugf("queue begin push ")
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
	if timeOnQueue > time.Duration(2)*time.Second {
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

func handleDiscoveryRequests() {
	discoveryQueue = CreateOrReturnQueue("DEFAULT")

	// create a pool of discovery workers
	for i := uint(0); i < 10; i++ {
		go func() {
			for {
				instanceKey := discoveryQueue.Consume()
				// Possibly this used to be the elected node, but has
				// been demoted, while still the queue is full.
				//if !IsLeaderOrActive() {
				//	log.Debugf("Node apparently demoted. Skipping discovery of %+v. "+
				//		"Remaining queue size: %+v", instanceKey, discoveryQueue.QueueLen())
				//	discoveryQueue.Release(instanceKey)
				//	continue
				//}

				DiscoverInstance(instanceKey)
				discoveryQueue.Release(instanceKey)
			}
		}()
	}
}

func DiscoverInstance(instanceKey string) {
	println("consume key is :", instanceKey)
	time.Sleep(time.Duration(10) * time.Second)
}

func putsomekey() {
	for i := 0; i < 100; i++ {

		discoveryQueue.Push("key" + strconv.Itoa(i))
	}
}
func main() {
	handleDiscoveryRequests()
	time.Sleep(time.Duration(5) * time.Second)
	go putsomekey()

	time.Sleep(time.Duration(5) * time.Second)
	StopMonitoring()

}
