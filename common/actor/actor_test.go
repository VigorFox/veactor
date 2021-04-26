package actor

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

func TestActorCreateAndStop(t *testing.T) {

	actorNum := 100
	actorSlice := []*Actor{}
	for i := 0; i < actorNum; i++ {
		a := NewActor(false)
		a.Start()

		msg := fmt.Sprintf("Actor %d is Running", i)
		a.PostAndProcessMessage(
			func(args ...interface{}) {
				t.Logf(msg)
			})

		actorSlice = append(actorSlice, a)
	}

	time.Sleep(45 * time.Millisecond)

	for i, a := range actorSlice {
		t.Logf("Stopping Actor %d", i)
		a.Stop()
	}
}

func TestActorProcessingPower(t *testing.T) {
	testActor1 := NewActor(false)
	testActor1.Start()

	testActor2 := NewActor(false)
	testActor2.Start()

	var f1 ProcessMessageFunc
	var f2 ProcessMessageFunc
	counter1 := 0
	f1 = func(args ...interface{}) {
		counter1++
		// 交叉投递消息
		err := testActor2.PostAndProcessMessage(f2)
		if err != nil {
			t.Logf(err.Error())
		}
	}

	counter2 := 0
	f2 = func(args ...interface{}) {
		counter2++
		// 交叉投递消息
		err := testActor1.PostAndProcessMessage(f1)
		if err != nil {
			t.Logf(err.Error())
		}
	}

	testActor1.PostAndProcessMessage(f1)
	testActor2.PostAndProcessMessage(f2)

	time.Sleep(time.Second)

	testActor1.Stop()
	testActor2.Stop()

	t.Logf("cnt1: %d ", counter1)
	t.Logf("cnt2: %d ", counter2)

	// 测试持续投递消息
	testActor3 := NewActor(false)
	testActor3.Start()

	stopped := false
	counter3 := 0
	f3 := func(args ...interface{}) {
		counter3++
	}

	go func() {
		for !stopped {
			testActor3.PostAndProcessMessage(f3)
		}
	}()

	time.Sleep(time.Second)
	stopped = true

	t.Logf("cnt3: %d ", counter3)
}

func TestActorSelfStop(t *testing.T) {
	testActor := NewActor(false)
	testActor.Start()

	f := func(args ...interface{}) {
		testActor.StopLater()
	}

	testActor.PostAndProcessMessage(f)

	time.Sleep(time.Second)
}

func TestActorTimer(t *testing.T) {
	testActor := NewActor(false)
	testActor.Start()

	t.Logf("startTime: %v", time.Now())

	cnt := 0

	f := func(args ...interface{}) bool {
		t.Logf("active timer time: %v", time.Now())
		cnt++

		return cnt >= 2 // timer 被激活 2 次后销毁
	}

	testActor.AddTimer(300, f)

	time.Sleep(time.Second)
}

func TestAddManyActorTimer(t *testing.T) {
	testActor := NewActor(false)
	testActor.Start()

	currNum := int32(0)

	f := func(args ...interface{}) bool {
		curID := args[0].(int)
		if int(atomic.LoadInt32(&currNum)) != curID {
			t.Errorf("actor timer active not in queue")
		}
		atomic.AddInt32(&currNum, 1)
		return true
	}

	for i := 0; i < 1000; i++ {
		testActor.AddTimer(i, f, i)
	}

	time.Sleep(time.Second * 2)

	if atomic.LoadInt32(&currNum) != 1000 {
		t.Errorf("actor timer active err: exec num not as excepted")
	}
}

func TestRemoveActorTimer(t *testing.T) {
	testActor := NewActor(false)
	testActor.Start()

	t.Logf("startTime: %v", time.Now())

	f := func(args ...interface{}) bool {
		t.Errorf("the timer should be removed")
		return false
	}

	timerID := testActor.AddTimer(1000, f)

	time.Sleep(time.Millisecond * 100)

	testActor.RemoveTimer(timerID)

	time.Sleep(time.Second)
}

func TestRemoveTimerInActor(t *testing.T) {
	testActor := NewActor(false)
	testActor.Start()

	t.Logf("startTime: %v", time.Now())

	f := func(args ...interface{}) bool {
		t.Errorf("the timer should be removed")
		return false
	}

	timerID := testActor.AddTimer(1000, f)

	time.Sleep(time.Millisecond * 100)

	testActor.PostAndProcessMessage(
		func(args ...interface{}) {
			testActor.RemoveTimer(timerID)
		})

	time.Sleep(time.Second)
}

func TestActorPostMsgWithArgs(t *testing.T) {
	testActor := NewActor(false)
	testActor.Start()

	f := func(args ...interface{}) {
		if len(args) != 2 {
			t.Error("invalid param cnt")
		}

		n := args[0].(int)
		m := args[1].(int)

		t.Log(n, m)
	}

	testActor.PostAndProcessMessage(f, 10, 321)

	time.Sleep(time.Millisecond * 100)
}

func TestStartActorTwice(t *testing.T) {
	testActor := NewActor(false)
	var err error
	err = testActor.Start()
	if err != nil {
		t.Log(err)
	}
	err = testActor.Start()
	if err != nil {
		t.Log(err)
	}
	testActor.Stop()

	err = testActor.Start()
	if err != nil {
		t.Log(err)
	}
}

func TestStopActorTwice(t *testing.T) {
	testActor := NewActor(false)
	var err error

	err = testActor.Stop()
	if err != nil {
		t.Log(err)
	}

	err = testActor.Start()
	if err != nil {
		t.Log(err)
	}
	err = testActor.Stop()
	if err != nil {
		t.Log(err)
	}

	err = testActor.Stop()
	if err != nil {
		t.Log(err)
	}
}

func TestLockOSThread(t *testing.T) {
	testActor := NewActor(true)
	var err error

	err = testActor.Start()
	if err != nil {
		t.Log(err)
	}

	err = testActor.Stop()
	if err != nil {
		t.Log(err)
	}
}

func TestStartActorPanic(t *testing.T) {
	testActor := NewActor(false)
	err := testActor.Start()
	if err != nil {
		t.Log(err)
	}

	f := func(args ...interface{}) {
		a := []int{0, 1, 2, 3}

		fmt.Printf("%d", a[4])
	}

	testActor.PostAndProcessMessage(f)

	testActor.Stop()
}
