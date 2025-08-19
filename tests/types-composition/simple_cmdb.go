package main

import (
	"context"
	"fmt"

	"github.com/foliagecp/easyjson"
	lg "github.com/foliagecp/sdk/statefun/logger"
	"github.com/foliagecp/sdk/statefun/system"
)

func CreateTestCmdb() {
	le := lg.GetLogger()
	le.Info(context.TODO(), "===========>Create Test CMDB")

	system.MsgOnErrorReturn(dbClient.CMDB.TypeCreate("datacenter"))
	system.MsgOnErrorReturn(dbClient.CMDB.TypeCreate("rack"))
	system.MsgOnErrorReturn(dbClient.CMDB.TypeCreate("server"))
	system.MsgOnErrorReturn(dbClient.CMDB.TypeCreate("storage"))
	system.MsgOnErrorReturn(dbClient.CMDB.TypeCreate("cpu"))
	system.MsgOnErrorReturn(dbClient.CMDB.TypeCreate("gpu"))
	system.MsgOnErrorReturn(dbClient.CMDB.TypeCreate("memory"))
	system.MsgOnErrorReturn(dbClient.CMDB.TypeCreate("network_card"))

	system.MsgOnErrorReturn(dbClient.CMDB.TypeCreate("hypervisor"))
	system.MsgOnErrorReturn(dbClient.CMDB.TypeCreate("virtual_machine"))
	system.MsgOnErrorReturn(dbClient.CMDB.TypeCreate("container"))
	system.MsgOnErrorReturn(dbClient.CMDB.TypeCreate("network"))
	system.MsgOnErrorReturn(dbClient.CMDB.TypeCreate("volume"))
	system.MsgOnErrorReturn(dbClient.CMDB.TypeCreate("service"))

	system.MsgOnErrorReturn(dbClient.CMDB.TypesLinkCreate("datacenter", "rack", "dc_rack", []string{"contains"}))
	system.MsgOnErrorReturn(dbClient.CMDB.TypesLinkCreate("rack", "server", "rack_srv", []string{"contains"}))
	system.MsgOnErrorReturn(dbClient.CMDB.TypesLinkCreate("server", "storage", "srv_stor", []string{"storage"}))
	system.MsgOnErrorReturn(dbClient.CMDB.TypesLinkCreate("server", "cpu", "srv_cpu", []string{"cpu"}))
	system.MsgOnErrorReturn(dbClient.CMDB.TypesLinkCreate("server", "gpu", "srv_gpu", []string{"gpu"}))
	system.MsgOnErrorReturn(dbClient.CMDB.TypesLinkCreate("server", "memory", "srv_mem", []string{"memory"}))
	system.MsgOnErrorReturn(dbClient.CMDB.TypesLinkCreate("server", "network_card", "srv_net", []string{"network"}))

	system.MsgOnErrorReturn(dbClient.CMDB.TypesLinkCreate("server", "hypervisor", "srv_hyp", []string{"runs"}))
	system.MsgOnErrorReturn(dbClient.CMDB.TypesLinkCreate("hypervisor", "virtual_machine", "hyp_vm", []string{"hosts"}))
	system.MsgOnErrorReturn(dbClient.CMDB.TypesLinkCreate("virtual_machine", "container", "vm_cont", []string{"runs"}))
	system.MsgOnErrorReturn(dbClient.CMDB.TypesLinkCreate("hypervisor", "network", "hyp_net", []string{"network"}))
	system.MsgOnErrorReturn(dbClient.CMDB.TypesLinkCreate("hypervisor", "volume", "hyp_vol", []string{"storage"}))
	system.MsgOnErrorReturn(dbClient.CMDB.TypesLinkCreate("container", "service", "cont_svc", []string{"service"}))

	dcBody1 := easyjson.NewJSONObject()
	dcBody1.SetByPath("SN", easyjson.NewJSON("DC001"))
	dcBody1.SetByPath("location", easyjson.NewJSON("New York"))
	system.MsgOnErrorReturn(dbClient.CMDB.ObjectCreate("datacenter1", "datacenter", dcBody1))

	dcBody2 := easyjson.NewJSONObject()
	dcBody2.SetByPath("SN", easyjson.NewJSON("DC002"))
	dcBody2.SetByPath("location", easyjson.NewJSON("London"))
	system.MsgOnErrorReturn(dbClient.CMDB.ObjectCreate("datacenter2", "datacenter", dcBody2))

	for i := 1; i <= 4; i++ {
		rackBody := easyjson.NewJSONObject()
		rackBody.SetByPath("SN", easyjson.NewJSON(fmt.Sprintf("RACK%03d", i)))
		rackName := fmt.Sprintf("rack%d", i)
		dcName := "datacenter1"
		if i > 2 {
			dcName = "datacenter2"
		}
		system.MsgOnErrorReturn(dbClient.CMDB.ObjectCreate(rackName, "rack", rackBody))
		system.MsgOnErrorReturn(dbClient.CMDB.ObjectsLinkCreate(dcName, rackName, fmt.Sprintf("dc-rack%d", i), []string{"tag1"}))
	}

	for i := 1; i <= 6; i++ {
		serverBody := easyjson.NewJSONObject()
		serverBody.SetByPath("SN", easyjson.NewJSON(fmt.Sprintf("SE%04d", i)))
		serverName := fmt.Sprintf("server%d", i)
		rackName := fmt.Sprintf("rack%d", ((i-1)/2)+1)

		system.MsgOnErrorReturn(dbClient.CMDB.ObjectCreate(serverName, "server", serverBody))
		system.MsgOnErrorReturn(dbClient.CMDB.ObjectsLinkCreate(rackName, serverName, fmt.Sprintf("rack-srv%d", i), []string{"tag1"}))

		cpuBody := easyjson.NewJSONObject()
		cpuBody.SetByPath("SN", easyjson.NewJSON(fmt.Sprintf("CPU%04d", i)))
		cpuName := fmt.Sprintf("cpu%d", i)
		system.MsgOnErrorReturn(dbClient.CMDB.ObjectCreate(cpuName, "cpu", cpuBody))
		system.MsgOnErrorReturn(dbClient.CMDB.ObjectsLinkCreate(serverName, cpuName, fmt.Sprintf("srv%d-cpu", i), []string{"tag1"}))

		memBody := easyjson.NewJSONObject()
		memBody.SetByPath("SN", easyjson.NewJSON(fmt.Sprintf("MEM%04d", i)))
		memName := fmt.Sprintf("memory%d", i)
		system.MsgOnErrorReturn(dbClient.CMDB.ObjectCreate(memName, "memory", memBody))
		system.MsgOnErrorReturn(dbClient.CMDB.ObjectsLinkCreate(serverName, memName, fmt.Sprintf("srv%d-mem", i), []string{"tag1"}))

		netBody := easyjson.NewJSONObject()
		netBody.SetByPath("SN", easyjson.NewJSON(fmt.Sprintf("NET%04d", i)))
		netName := fmt.Sprintf("network_card%d", i)
		system.MsgOnErrorReturn(dbClient.CMDB.ObjectCreate(netName, "network_card", netBody))
		system.MsgOnErrorReturn(dbClient.CMDB.ObjectsLinkCreate(serverName, netName, fmt.Sprintf("srv%d-net", i), []string{"tag1"}))

		for j := 1; j <= 2; j++ {
			storageBody := easyjson.NewJSONObject()
			storageBody.SetByPath("SN", easyjson.NewJSON(fmt.Sprintf("ST%04d", (i-1)*2+j)))
			storageName := fmt.Sprintf("storage%d_%d", i, j)
			system.MsgOnErrorReturn(dbClient.CMDB.ObjectCreate(storageName, "storage", storageBody))
			system.MsgOnErrorReturn(dbClient.CMDB.ObjectsLinkCreate(serverName, storageName, fmt.Sprintf("srv%d-st%d", i, j), []string{"tag1"}))
		}

		if i%2 == 1 {
			gpuBody := easyjson.NewJSONObject()
			gpuBody.SetByPath("SN", easyjson.NewJSON(fmt.Sprintf("GPU%04d", i)))
			gpuName := fmt.Sprintf("gpu%d", i)
			system.MsgOnErrorReturn(dbClient.CMDB.ObjectCreate(gpuName, "gpu", gpuBody))
			system.MsgOnErrorReturn(dbClient.CMDB.ObjectsLinkCreate(serverName, gpuName, fmt.Sprintf("srv%d-gpu", i), []string{"tag1"}))
		}

		hypBody := easyjson.NewJSONObject()
		hypName := fmt.Sprintf("hypervisor%d", i)
		system.MsgOnErrorReturn(dbClient.CMDB.ObjectCreate(hypName, "hypervisor", hypBody))
		system.MsgOnErrorReturn(dbClient.CMDB.ObjectsLinkCreate(serverName, hypName, fmt.Sprintf("srv%d-hyp", i), []string{"tag1"}))

		for k := 1; k <= 2; k++ {
			netVirtBody := easyjson.NewJSONObject()
			netVirtName := fmt.Sprintf("network%d_%d", i, k)
			system.MsgOnErrorReturn(dbClient.CMDB.ObjectCreate(netVirtName, "network", netVirtBody))
			system.MsgOnErrorReturn(dbClient.CMDB.ObjectsLinkCreate(hypName, netVirtName, fmt.Sprintf("hyp%d-net%d", i, k), []string{"tag1"}))
		}

		for k := 1; k <= 3; k++ {
			volBody := easyjson.NewJSONObject()
			volName := fmt.Sprintf("volume%d_%d", i, k)
			system.MsgOnErrorReturn(dbClient.CMDB.ObjectCreate(volName, "volume", volBody))
			system.MsgOnErrorReturn(dbClient.CMDB.ObjectsLinkCreate(hypName, volName, fmt.Sprintf("hyp%d-vol%d", i, k), []string{"tag1"}))
		}

		for j := 1; j <= 3; j++ {
			vmBody := easyjson.NewJSONObject()
			vmName := fmt.Sprintf("vm%d_%d", i, j)
			system.MsgOnErrorReturn(dbClient.CMDB.ObjectCreate(vmName, "virtual_machine", vmBody))
			system.MsgOnErrorReturn(dbClient.CMDB.ObjectsLinkCreate(hypName, vmName, fmt.Sprintf("hyp%d-vm%d", i, j), []string{"tag1"}))

			for k := 1; k <= 2; k++ {
				contBody := easyjson.NewJSONObject()
				contName := fmt.Sprintf("container%d_%d_%d", i, j, k)
				system.MsgOnErrorReturn(dbClient.CMDB.ObjectCreate(contName, "container", contBody))
				system.MsgOnErrorReturn(dbClient.CMDB.ObjectsLinkCreate(vmName, contName, fmt.Sprintf("vm%d_%d-cont%d", i, j, k), []string{"tag1"}))

				for l := 1; l <= 2; l++ {
					svcBody := easyjson.NewJSONObject()
					svcName := fmt.Sprintf("service%d_%d_%d_%d", i, j, k, l)
					system.MsgOnErrorReturn(dbClient.CMDB.ObjectCreate(svcName, "service", svcBody))
					system.MsgOnErrorReturn(dbClient.CMDB.ObjectsLinkCreate(contName, svcName, fmt.Sprintf("cont%d_%d_%d-svc%d", i, j, k, l), []string{"tag1"}))
				}
			}
		}
	}

	le.Info(context.TODO(), "==============Creating inventory type")
	system.MsgOnErrorReturn(dbClient.CMDB.TypeCreate("inventory"))
	system.MsgOnErrorReturn(dbClient.CMDB.TypesLinkCreate("inventory", "inventory", "inv-inv", []string{"tag1"}))

	inventorySubTypes := []string{
		"datacenter", "rack", "server", "storage", "cpu", "gpu", "memory", "network_card",
	}

	for _, subType := range inventorySubTypes {
		system.MsgOnErrorReturn(dbClient.CMDB.TypeSetSubType("inventory", subType))
	}

	le.Info(context.TODO(), "<===========Test CMDB Created")
}
