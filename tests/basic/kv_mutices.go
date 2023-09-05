

package basic

import (
	"fmt"
	"sync"
	"time"

	"github.com/foliagecp/sdk/statefun"
)

func KVMuticesSimpleTest(runtime *statefun.Runtime, testDurationSec int, workersCount int, workTimeMs int, afterWorkTimeMs int) {
	fmt.Println(">>> Test started: kv mutices")

	wg := new(sync.WaitGroup)
	stop := make(chan bool)

	testFunc := func(i int) {
		defer wg.Done()

		s := fmt.Sprintf("W%d", i)
		for true {
			v, err := statefun.KeyMutexLock(runtime, "test.object", false, s)
			if err != nil {
				fmt.Printf("ERR KeyMutexLock test: %v\n", err)
			}
			time.Sleep(time.Duration(workTimeMs) * time.Millisecond)
			err = statefun.KeyMutexUnlock(runtime, "test.object", v, s)
			if err != nil {
				fmt.Printf("ERR KeyMutexUnlock test: %v\n", err)
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

	fmt.Println("<<< Test ended: kv mutices")
}
