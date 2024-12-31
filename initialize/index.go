package initialize

import "sync"

func Run() {
	listSync := []func(){
		runAppCommon,
	}

	for _, f := range listSync {
		f()
	}

	listAsync := []func(){
		initProcessStream,
		runGRPC,
	}

	var wg sync.WaitGroup
	for _, f := range listAsync {
		wg.Add(1)
		go func(f func()) {
			defer wg.Done()
			f()
		}(f)
	}

	wg.Wait()
}
