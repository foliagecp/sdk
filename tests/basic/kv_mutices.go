

package basic

import (
	"fmt"
	"sync"
	"time"

	"github.com/foliagecp/sdk/statefun"
	lg "github.com/foliagecp/sdk/statefun/logger"
)

func KVMuticesSimpleTest(runtime *statefun.Runtime, testDurationSec int, workersCount int, workTimeMs int, afterWorkTimeMs int) {
	lg.Logln(lg.DebugLevel, ">>> Test started: kv mutices")

	wg := new(sync.WaitGroup)
	stop := make(chan bool)

	testFunc := func(i int) {
		defer wg.Done()

		s := fmt.Sprintf("W%d", i)
		for {
			v, err := statefun.KeyMutexLock(runtime, "test.object", false, s)
			if err != nil {
				lg.Logf(lg.ErrorLevel, "KeyMutexLock test: %v\n", err)
			}
			time.Sleep(time.Duration(workTimeMs) * time.Millisecond)
			err = statefun.KeyMutexUnlock(runtime, "test.object", v, s)
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
