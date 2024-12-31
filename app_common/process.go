package appcommon

import (
	"errors"
	"sync"
)

var mutexProcess sync.Mutex

func makeMapProcess() {
	processStream = map[string](chan string){}
}

func CreateProcess(uuidProcess string) error {
	if processStream == nil {
		return errors.New("map process stream null")
	}
	mutexProcess.Lock()
	processStream[uuidProcess] = make(chan string, 1*100*100)
	mutexProcess.Unlock()
	chanListenAddProcessStream <- uuidProcess

	return nil
}

func GetProcessStream(uuidProcess string) chan string {
	mutexProcess.Lock()
	process := processStream[uuidProcess]
	mutexProcess.Unlock()

	return process
}

func GetChanListenAddProcessStream() chan string {
	return chanListenAddProcessStream
}
