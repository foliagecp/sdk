package main

import (
	"fmt"

	lg "github.com/foliagecp/sdk/statefun/logger"
	"github.com/foliagecp/sdk/statefun/system"

	"github.com/foliagecp/sdk/statefun"
)

func CreateTestGraph(runtime *statefun.Runtime) {
	lg.Logln(lg.DebugLevel, ">>> Test started: simple graph creation")

	for _, v := range []int{10, 100, 1000, 10000} {
		lg.Logf(lg.DebugLevel, "Creating %d servers", v)
		typeName := fmt.Sprintf("server%d", v)
		system.MsgOnErrorReturn(dbClient.CMDB.TypeCreate(typeName))

		for i := 1; i < v; i++ {
			objectName := fmt.Sprintf("server%d-%d", v, i)
			system.MsgOnErrorReturn(dbClient.CMDB.ObjectCreate(objectName, typeName))
		}
	}

	lg.Logln(lg.DebugLevel, "<<< Test ended: simple graph creation")
}
