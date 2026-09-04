package registry

import (
	"testing"
	"time"
)

// A delayed old timer must not lose a state change suppressed by a newer
// window. The first two plans see insufficient reload memory; only the final
// heartbeat supplies enough, and no further heartbeat follows it.
func TestDelayedTrailingPlanPreservesNewWindowHeartbeat(t *testing.T) {
	reg := New(testLogger())
	loads := make(chan string, 4)
	reg.loadModelSender = func(providerID, model string) error {
		loads <- providerID + "/" + model
		return nil
	}
	const model = "swap-delayed-trailing-model"
	reg.SetModelCatalog([]CatalogEntry{{ID: model, SizeGB: 8, MinRAMGB: 16}})
	p := makeSchedulerProvider(t, reg, "p1", model, 100)
	reg.Heartbeat(p.ID, swapTestCrashedHeartbeat(model, 1))
	if err := reg.Queue().Enqueue(swapTestQueued("swap-delayed-trailing-request", model)); err != nil {
		t.Fatal(err)
	}
	stub := installTrailingTimerStub(reg)
	t0 := time.Now()
	now := t0
	reg.swapPlanGate.now = func() time.Time { return now }
	reg.triggerModelSwapsFromHeartbeat(t0)
	reg.triggerModelSwapsFromHeartbeat(t0.Add(100 * time.Millisecond))
	if len(stub.fire) != 1 {
		t.Fatal("suppressed heartbeat did not arm the old timer")
	}
	// Its timer is delayed; another heartbeat plans after the window opens.
	newWindow := t0.Add(modelSwapPlanInterval + 10*time.Millisecond)
	if !reg.triggerModelSwapsFromHeartbeat(newWindow) {
		t.Fatal("new heartbeat did not open a new planning window")
	}
	select {
	case got := <-loads:
		t.Fatalf("planned a reload with insufficient memory: %s", got)
	default:
	}
	// Simulate the heartbeat's state mutation and planner notification while
	// the old timer is still armed. A crashed slot cannot drain this request.
	p.mu.Lock()
	freeForLoadGB := 32.0
	p.BackendCapacity.FreeForLoadGB = &freeForLoadGB
	p.mu.Unlock()
	reg.triggerModelSwapsFromHeartbeat(newWindow.Add(10 * time.Millisecond))
	now = newWindow.Add(20 * time.Millisecond)
	stub.fire[0]()
	if len(stub.fire) != 2 || stub.waits[1] != modelSwapPlanInterval-20*time.Millisecond {
		t.Fatalf("old timer lost the new window's heartbeat: waits %v", stub.waits)
	}
	now = newWindow.Add(modelSwapPlanInterval)
	stub.fire[1]()
	select {
	case got := <-loads:
		if want := p.ID + "/" + model; got != want {
			t.Fatalf("load = %s, want %s", got, want)
		}
	default:
		t.Fatal("no reload after the delayed timer's follow-up")
	}
	if reg.swapPlanGate.planRuns() != 3 || reg.swapPlanGate.trailingArmed() {
		t.Fatal("trailing callback did not finish the third plan")
	}
}
