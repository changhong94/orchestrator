package orch

import "testing"

func TestPreparedRestorePoolCannotClaimBootedOrColdSlots(t *testing.T) {
	cold := &runPool{}
	booted := &runPool{preparedVM: true}
	restore := &runPool{preparedVM: true, preparedRestore: true}
	pools := &runPools{pools: []*runPool{cold, booted, restore}}
	got := pools.PreparedPool(true)
	if len(got.pools) != 1 || got.pools[0] != restore {
		t.Fatal("restore selected incompatible pool")
	}
	got = pools.PreparedPool()
	if len(got.pools) != 1 || got.pools[0] != booted {
		t.Fatal("image launch selected restore holder")
	}
}
