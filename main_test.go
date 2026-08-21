package main

import "testing"

func TestChooseSlot(t *testing.T) {
	request := request{Compute: 30, Mem: 20}
	slots := []slotMetrics{
		{Slot: 0, ComputeUsed: 80, MemUsed: 20, Jobs: 1},
		{Slot: 1, ComputeUsed: 60, MemUsed: 70, Jobs: 2},
		{Slot: 2},
	}

	packed, err := chooseSlot(slots, request, false)
	if err != nil || packed != 1 {
		t.Fatalf("pack: got slot %d, err %v; want slot 1", packed, err)
	}

	naive, err := chooseSlot(slots, request, true)
	if err != nil || naive != 2 {
		t.Fatalf("naive: got slot %d, err %v; want empty slot 2", naive, err)
	}
}

func TestChooseSlotRejectsWhenBothResourcesDoNotFit(t *testing.T) {
	slots := []slotMetrics{{Slot: 0, ComputeUsed: 10, MemUsed: 90, Jobs: 1}}
	if _, err := chooseSlot(slots, request{Compute: 20, Mem: 20}, false); err == nil {
		t.Fatal("expected request to be rejected when memory capacity is exhausted")
	}
}
