package worker

import (
	"context"
	"testing"

	"github.com/fardeensayyed16/orca/pkg/types"
)

func TestPortPoolAllocatesLowestAvailableAndPersists(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	pool, err := newPortPool("40000-40002")
	if err != nil {
		t.Fatal(err)
	}
	first, err := pool.acquire(context.Background(), "replica-a")
	if err != nil {
		t.Fatal(err)
	}
	second, err := pool.acquire(context.Background(), "replica-b")
	if err != nil {
		t.Fatal(err)
	}
	if first != 40000 || second != 40001 {
		t.Fatalf("allocated ports %d and %d, want 40000 and 40001", first, second)
	}
	if err := pool.close(); err != nil {
		t.Fatal(err)
	}

	pool, err = newPortPool("40000-40002")
	if err != nil {
		t.Fatal(err)
	}
	defer pool.close()
	persisted, err := pool.acquire(context.Background(), "replica-a")
	if err != nil {
		t.Fatal(err)
	}
	if persisted != 40000 {
		t.Fatalf("persisted port = %d, want 40000", persisted)
	}
	if err := pool.release(context.Background(), "replica-a"); err != nil {
		t.Fatal(err)
	}
	reused, err := pool.acquire(context.Background(), "replica-c")
	if err != nil {
		t.Fatal(err)
	}
	if reused != 40000 {
		t.Fatalf("reused port = %d, want lowest free port 40000", reused)
	}
}

func TestRequiresRestartPolicy(t *testing.T) {
	cases := []struct {
		policy types.RestartPolicy
		exit   int
		want   bool
	}{
		{types.RestartPolicyAlways, 0, true},
		{types.RestartPolicyAlways, 1, true},
		{types.RestartPolicyOnFailure, 0, false},
		{types.RestartPolicyOnFailure, 1, true},
		{types.RestartPolicyNever, 1, false},
	}
	for _, test := range cases {
		if got := requiresRestart(test.policy, test.exit); got != test.want {
			t.Errorf("requiresRestart(%q, %d) = %v, want %v", test.policy, test.exit, got, test.want)
		}
	}
}
