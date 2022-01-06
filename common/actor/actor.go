// Description: actor implementation in go
// Author: LIANG YUXUAN
// Since: 2021-04-12
package actor

import (
	"container/heap"
	"fmt"
	"os"
	"runtime"
	"sync/atomic"
	"time"

	"github.com/VigorFox/veactor/common/actor/queue/mpsc"
	"github.com/VigorFox/veactor/common/runtime/stack"

	"github.com/rpccloud/goid"
)

var DefaultHandler = func(err any) {
	fmt.Fprintf(os.Stderr, "actor panic recovered: %s\n%s\n", err, stack.CallStack(3))
}

// TimerCB 定时器回调函数模板
type TimerCB func(arg any) bool

// Timer 通用定时器对象
type Timer struct {
	ID               int64
	Interval         time.Duration
	LastActivateTime time.Time
	CallBack         TimerCB
	CallBackParam    any
}

// timerHeap 定时器实现所使用的堆结构
type timerHeap []*Timer

// timerList 定时器列表对象
type timerList struct {
	lookup map[int64]*Timer
	heap   *timerHeap
}

// Len 获取堆大小，堆排序接口实现
func (th *timerHeap) Len() int {
	return len(*th)
}

// Less 元素之间大小比较函数，堆排序接口实现
func (th *timerHeap) Less(i, j int) bool {
	timer1 := (*th)[i]
	timer2 := (*th)[j]
	return timer1.LastActivateTime.Add(timer1.Interval).Before(
		timer2.LastActivateTime.Add(timer2.Interval))
}

// Swap 堆元素交换，堆排序接口实现
func (th *timerHeap) Swap(i, j int) {
	(*th)[i], (*th)[j] = (*th)[j], (*th)[i]
}

// Push 向堆末尾追加元素。
// 如果堆容量不足，创建一个原容量两倍的堆，把原堆元素拷贝到新堆，再添加新元素。
// 堆排序接口实现
func (th *timerHeap) Push(x any) {
	if th == nil {
		panic("timerHeap.Push Err: trying to access nil timerHeap pointer")
	}
	n := len(*th)
	c := cap(*th)
	if th == nil || c == 0 {
		*th = make(timerHeap, 0, 16)
	} else if n+1 > c {
		nth := make(timerHeap, n, c*2)
		copy(nth, *th)
		*th = nth
	}
	*th = (*th)[0 : n+1]
	timer := x.(*Timer)
	(*th)[n] = timer
}

// Pop 弹出一个元素，堆排序接口实现
func (th *timerHeap) Pop() any {
	n := len(*th)
	c := cap(*th)
	if n < (c/4) && c > 25 {
		nth := make(timerHeap, n, c/2)
		copy(nth, *th)
		*th = nth
	}
	timer := (*th)[n-1]
	*th = (*th)[0 : n-1]
	return timer
}

// PeekAndShift 弹出堆顶元素，并对堆重排。
func (th *timerHeap) PeekAndShift(minTime time.Time) *Timer {
	if th.Len() == 0 {
		return nil
	}

	timer := (*th)[0]
	if timer.LastActivateTime.Add(timer.Interval).After(minTime) {
		return nil
	}
	heap.Remove(th, 0)

	return timer
}

// Insert 向堆插入一个元素并重排
func (tl *timerList) Insert(timer *Timer) {
	if timer == nil {
		return
	}
	if _, exists := tl.lookup[timer.ID]; exists {
		return
	}

	heap.Push(tl.heap, timer)
	tl.lookup[timer.ID] = timer
}

// ProcessMessageFunc 处理消息接口函数
type ProcessMessageFunc func(any)

// Message 消息结构
type Message struct {
	ProcessFunc ProcessMessageFunc
	FuncArg     any
}

type IMessageProcessor interface {
	ProcessMessage(*Message)
	Update(time.Time)
	Cleanup()
}

type DefaultMessageProcessor struct {
}

func (d *DefaultMessageProcessor) ProcessMessage(msg *Message) {
	if msg != nil && msg.ProcessFunc != nil {
		msg.ProcessFunc(msg.FuncArg)
	}
}

func (d *DefaultMessageProcessor) Update(time.Time) {
}

func (d *DefaultMessageProcessor) Cleanup() {
}

var defaultMessageProcessor = &DefaultMessageProcessor{}

const checkInterval = time.Second / 30

type Actor struct {
	goroutineID          int64
	lockOSThread         bool
	mailbox              *mpsc.Queue
	messageProcessor     IMessageProcessor
	isRunnning           uint32
	isClosing            uint32
	startTime            time.Time
	lastActiveUpdateTime time.Time
	lastActiveTime       time.Time
	timerIDCounter       int64
	tl                   *timerList

	// channels
	timerNotify         chan int
	mailNotify          chan int
	stopChan            chan int
	cleanupFinishedChan chan int
}

// NewActor 创建新的 Actor
// isLockOSThread 为 true 时，Actor 启动时会绑定操作系统的线程，通常而言效率更低
func NewActor(isLockOSThread bool) *Actor {
	actor := &Actor{
		-1,
		isLockOSThread,
		mpsc.New(),
		nil,
		0,
		0,
		time.Date(1970, 1, 1, 0, 0, 0, 0, time.Local),
		time.Date(1970, 1, 1, 0, 0, 0, 0, time.Local),
		time.Date(1970, 1, 1, 0, 0, 0, 0, time.Local),
		0,
		&timerList{make(map[int64]*Timer), &timerHeap{}},
		make(chan int, 16), // 暂定只为 timer 的channel 保留 16 个槽位
		make(chan int),
		make(chan int),
		make(chan int),
	}

	return actor
}

func (a *Actor) GetGoroutineID() int64 {
	return a.goroutineID
}

// AddTimer 添加定时器
func (a *Actor) AddTimer(interval int, callback TimerCB,
	callbackParam any) int64 {

	timerID := atomic.AddInt64(&a.timerIDCounter, 1)

	addTimerFunc := func() {
		timer := &Timer{}
		timer.ID = timerID
		timer.Interval = time.Duration(interval) * time.Millisecond
		timer.CallBack = callback
		timer.CallBackParam = callbackParam
		timer.LastActivateTime = a.lastActiveTime

		a.tl.Insert(timer)
	}

	if a.GetGoroutineID() == goid.GetRoutineId() {
		addTimerFunc()
	} else {
		a.PostAndProcessMessage(
			func(arg any) {
				addTimerFunc()
			}, nil)
	}

	return timerID
}

// RemoveTimer 移除定时器。
// 如果是在 timer 所在 actor 的 goroutine 外调用本函数，且 actor 的消息过多，
// 无法保证被移除的 timer 不会继续执行。
func (a *Actor) RemoveTimer(timerID int64) {
	delTimerFunc := func() {
		if timer, exists := a.tl.lookup[timerID]; exists {
			delete(a.tl.lookup, timerID)
			timer.CallBack = nil
			timer.CallBackParam = nil
		}
	}

	if a.GetGoroutineID() == goid.GetRoutineId() {
		delTimerFunc()
	} else {
		a.PostAndProcessMessage(
			func(arg any) {
				delTimerFunc()
			}, nil)
	}
}

// checkTimers 检查所有定时器，调用频率同与务自身 Actor 的帧率相同；只能在本 actor 所在线程调用，否则会抛出 panic
func (a *Actor) checkTimers() {
	nowTime := a.lastActiveTime
	for {
		timer := a.tl.heap.PeekAndShift(nowTime)
		if timer == nil {
			break
		}
		var removeTimer bool
		if timer.CallBack != nil {
			removeTimer = timer.CallBack(timer.CallBackParam)
		}
		if removeTimer {
			delete(a.tl.lookup, timer.ID)
		} else {
			timer.LastActivateTime = nowTime
			heap.Push(a.tl.heap, timer)
		}
	}
}

func (a *Actor) runTimerThread() {
	timerTicker := time.NewTicker(checkInterval)
	defer timerTicker.Stop()
	for a.isRunnning > 0 {
		<-timerTicker.C
		select {
		case a.timerNotify <- 0:
		default:
		}
	}
}

func (a *Actor) run() {
	go a.runTimerThread()

	if a.lockOSThread {
		runtime.LockOSThread()
	}

	a.goroutineID = goid.GetRoutineId()

	popAndProcessAllMsg := func() {
		hasGetTime := false
		for {
			msgInterface := a.mailbox.Pop()
			if msgInterface == nil {
				break
			}
			if !hasGetTime {
				a.lastActiveTime = time.Now()
				hasGetTime = true
			}
			msg := msgInterface.(*Message)
			a.processMessageSafe(msg)
		}
	}

	for a.isRunnning > 0 {
		select {
		case <-a.timerNotify:
			a.lastActiveTime = time.Now()
			a.lastActiveUpdateTime = a.lastActiveTime
			a.updateSafe()
			popAndProcessAllMsg()
		case <-a.mailNotify:
			popAndProcessAllMsg()
		case <-a.stopChan:
			popAndProcessAllMsg()
			a.cleanupSafe()
			a.cleanupFinishedChan <- 0
			atomic.StoreUint32(&a.isRunnning, 0)
			return
		}
	}

	// 执行 StopLater 方法的流程
	popAndProcessAllMsg()
	atomic.StoreUint32(&a.isClosing, 1)
	a.cleanupSafe()
}

func (a *Actor) Start() error {
	if atomic.LoadUint32(&a.isRunnning) > 0 {
		return fmt.Errorf("actor already running, can not start actor again")
	}
	if atomic.LoadUint32(&a.isClosing) > 0 {
		return fmt.Errorf("actor is closing, can not start actor again")
	}
	atomic.StoreUint32(&a.isRunnning, 1)
	a.startTime = time.Now()
	go a.run()
	return nil
}

func (a *Actor) Stop() error {
	// 假如侦测到是 actor 自身的 goroutine 调用本函数，直接调用 StopLater 函数，
	// 避免因调用自身 channel 死锁
	if a.GetGoroutineID() == goid.GetRoutineId() {
		a.StopLater()
		return nil
	}
	if atomic.LoadUint32(&a.isRunnning) == 0 {
		return fmt.Errorf("actor is not running, can not stop actor")
	}
	if atomic.LoadUint32(&a.isClosing) > 0 {
		return fmt.Errorf("actor is closing, can not stop actor again")
	}
	atomic.StoreUint32(&a.isClosing, 1)
	a.stopChan <- 0
	<-a.cleanupFinishedChan
	return nil
}

func (a *Actor) SetMessageProcessor(processor IMessageProcessor) {
	a.messageProcessor = processor
}

func (a *Actor) StopLater() {
	atomic.StoreUint32(&a.isRunnning, 0)
}

func (a *Actor) updateSafe() {
	defer func() {
		if r := recover(); r != nil {
			DefaultHandler(r)
		}
	}()

	if a.messageProcessor != nil {
		a.messageProcessor.Update(a.lastActiveUpdateTime)
	} else {
		defaultMessageProcessor.Update(a.lastActiveUpdateTime)
	}
	a.checkTimers()
}

func (a *Actor) processMessageSafe(msg *Message) {
	defer func() {
		if r := recover(); r != nil {
			DefaultHandler(r)
		}
	}()

	if a.messageProcessor != nil {
		a.messageProcessor.ProcessMessage(msg)
	} else {
		defaultMessageProcessor.ProcessMessage(msg)
	}
}

func (a *Actor) cleanupSafe() {
	defer func() {
		if r := recover(); r != nil {
			DefaultHandler(r)
		}
	}()

	if a.messageProcessor != nil {
		a.messageProcessor.Cleanup()
	} else {
		defaultMessageProcessor.Cleanup()
	}
}

func (a *Actor) PostAndProcessMessage(f ProcessMessageFunc, arg any) error {
	if atomic.LoadUint32(&a.isRunnning) == 0 {
		return fmt.Errorf("actor is not running, can not post message")
	}
	if atomic.LoadUint32(&a.isClosing) > 0 {
		return fmt.Errorf("actor is closing, can not post message")
	}

	msg := &Message{f, arg}
	a.mailbox.Push(msg)

	select {
	case a.mailNotify <- 0:
	default:
	}

	return nil
}
