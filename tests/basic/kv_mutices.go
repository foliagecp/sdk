package main

import (
	"sync"
	"time"

	"github.com/foliagecp/sdk/statefun"
	lg "github.com/foliagecp/sdk/statefun/logger"
	"github.com/foliagecp/sdk/statefun/system"
)

func KVMuticesSimpleTest(runtime *statefun.Runtime, testDurationSec int, workersCount int, workTimeMs int, afterWorkTimeMs int) {
	lg.Logln(lg.DebugLevel, ">>> Test started: kv mutices")

	wg := new(sync.WaitGroup)
	stop := make(chan bool)

	testFunc := func(i int) {
		system.GlobalPrometrics.GetRoutinesCounter().Started("KVMuticesSimpleTest-testFunc")
		defer system.GlobalPrometrics.GetRoutinesCounter().Stopped("KVMuticesSimpleTest-testFunc")
		defer wg.Done()

		for {
			v, err := statefun.KeyMutexLock(runtime, "test.object", false)
			if err != nil {
				lg.Logf(lg.ErrorLevel, "KeyMutexLock test: %v\n", err)
			}
			time.Sleep(time.Duration(workTimeMs) * time.Millisecond)
			err = statefun.KeyMutexUnlock(runtime, "test.object", v)
			if err != nil {
				lg.Logf(lg.ErrorLevel, "KeyMutexUnlock test: %v\n", err)
			}
			time.Sleep(time.Duration(afterWorkTimeMs) * time.Millisecond)

			select {
			case <-stop:
				return
			default:
				continue
			}
		}
	}

	for i := 0; i < workersCount; i++ {
		wg.Add(1)
		go testFunc(i)
	}

	time.Sleep(time.Duration(testDurationSec) * time.Second)

	close(stop)
	wg.Wait()

	lg.Logln(lg.DebugLevel, "<<< Test ended: kv mutices")
}
