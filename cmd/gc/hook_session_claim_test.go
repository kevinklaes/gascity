package main

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/gastownhall/gascity/internal/beads"
	"github.com/gastownhall/gascity/internal/session"
)

// executionAnswerRow seeds one session row the way the execution backstop
// leaves it after escalating: latched for an incarnation and a bead.
func executionAnswerRow(t *testing.T, store beads.Store, overrides map[string]string) beads.Bead {
	t.Helper()
	meta := map[string]string{
		"session_name":                     "test-city--worker-1",
		"state":                            "active",
		"instance_token":                   "incarnation-1",
		executionClaimNudgeWorkKey:         "work-1",
		executionClaimNudgeStalledKey:      "2026-03-01T12:00:00Z",
		executionClaimNudgeStalledTokenKey: "incarnation-1",
	}
	for k, v := range overrides {
		meta[k] = v
	}
	row, err := store.Create(beads.Bead{Title: "session", Type: sessionBeadType, Status: "open", Metadata: meta})
	if err != nil {
		t.Fatalf("seeding the session row: %v", err)
	}
	return row
}

// TestHookMarkExecutionResumedGuards: the claim door records an answer only
// for the incarnation and the bead the backstop latched, and writes nothing in
// every other case, so an ordinary adoption costs one read.
func TestHookMarkExecutionResumedGuards(t *testing.T) {
	now := time.Date(2026, 3, 1, 12, 5, 0, 0, time.UTC)
	wantStamp := now.Format(time.RFC3339)
	for _, tc := range []struct {
		name      string
		row       map[string]string
		token     string
		beadID    string
		wantWrote bool
		wantStamp string
	}{
		{name: "the latched incarnation answers for the latched bead", token: "incarnation-1", beadID: "work-1", wantWrote: true, wantStamp: wantStamp},
		{name: "a caller that carries no instance token", token: "", beadID: "work-1", wantWrote: true, wantStamp: wantStamp},
		{name: "surrounding space on the ids", token: " incarnation-1 ", beadID: " work-1 ", wantWrote: true, wantStamp: wantStamp},
		{
			name:  "an answer older than a re-written latch is replaced",
			row:   map[string]string{executionClaimResumedKey: "2026-03-01T11:00:00Z"},
			token: "incarnation-1", beadID: "work-1", wantWrote: true, wantStamp: wantStamp,
		},
		{
			name:  "an answer already recorded is not rewritten",
			row:   map[string]string{executionClaimResumedKey: "2026-03-01T12:01:00Z"},
			token: "incarnation-1", beadID: "work-1", wantStamp: "2026-03-01T12:01:00Z",
		},
		{
			name:  "no latch",
			row:   map[string]string{executionClaimNudgeStalledKey: "", executionClaimNudgeStalledTokenKey: ""},
			token: "incarnation-1", beadID: "work-1",
		},
		{
			name:  "a latch with no incarnation",
			row:   map[string]string{executionClaimNudgeStalledTokenKey: "", "instance_token": ""},
			token: "", beadID: "work-1",
		},
		{
			name:  "the row was re-incarnated after the latch",
			row:   map[string]string{"instance_token": "incarnation-2"},
			token: "incarnation-2", beadID: "work-1",
		},
		{name: "the caller is another incarnation", token: "incarnation-0", beadID: "work-1"},
		{name: "another bead", token: "incarnation-1", beadID: "work-2"},
		{name: "no bead", token: "incarnation-1", beadID: ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := beads.NewMemStore()
			row := executionAnswerRow(t, store, tc.row)

			wrote, err := markExecutionResumed(sessionFrontDoor(store), row.ID, tc.token, tc.beadID, now)
			if err != nil {
				t.Fatalf("markExecutionResumed: %v", err)
			}
			if wrote != tc.wantWrote {
				t.Fatalf("wrote = %v, want %v", wrote, tc.wantWrote)
			}
			after, err := store.Get(row.ID)
			if err != nil {
				t.Fatalf("re-reading the session row: %v", err)
			}
			if got := strings.TrimSpace(after.Metadata[executionClaimResumedKey]); got != tc.wantStamp {
				t.Fatalf("%s = %q, want %q", executionClaimResumedKey, got, tc.wantStamp)
			}
			if tc.wantWrote && !executionStalledDrainResumed(after.Metadata) {
				t.Fatalf("the recorded answer does not satisfy executionStalledDrainResumed: %v", after.Metadata)
			}
		})
	}
}

// TestHookMarkExecutionResumedRefusesARowThatIsNotThisSession: the id is
// resolved exactly and must name an open session bead; anything else is
// refused before a write, and an unresolvable id is an error the caller can
// see.
func TestHookMarkExecutionResumedRefusesARowThatIsNotThisSession(t *testing.T) {
	now := time.Date(2026, 3, 1, 12, 5, 0, 0, time.UTC)

	t.Run("a work bead carrying the same keys", func(t *testing.T) {
		store := beads.NewMemStore()
		work, err := store.Create(beads.Bead{Title: "work", Type: "task", Metadata: map[string]string{
			"instance_token":                   "incarnation-1",
			executionClaimNudgeWorkKey:         "work-1",
			executionClaimNudgeStalledKey:      "2026-03-01T12:00:00Z",
			executionClaimNudgeStalledTokenKey: "incarnation-1",
		}})
		if err != nil {
			t.Fatalf("seeding the work bead: %v", err)
		}
		wrote, err := markExecutionResumed(sessionFrontDoor(store), work.ID, "incarnation-1", "work-1", now)
		if wrote || !errors.Is(err, session.ErrSessionNotFound) {
			t.Fatalf("wrote=%v err=%v, want a refusal with ErrSessionNotFound", wrote, err)
		}
		after, _ := store.Get(work.ID)
		if got := after.Metadata[executionClaimResumedKey]; got != "" {
			t.Fatalf("%s written on a non-session bead: %q", executionClaimResumedKey, got)
		}
	})

	t.Run("an absent session", func(t *testing.T) {
		wrote, err := markExecutionResumed(sessionFrontDoor(beads.NewMemStore()), "gcg-session-absent", "incarnation-1", "work-1", now)
		if wrote || err == nil {
			t.Fatalf("wrote=%v err=%v, want an error for an absent session", wrote, err)
		}
	})

	t.Run("a closed session", func(t *testing.T) {
		store := beads.NewMemStore()
		row := executionAnswerRow(t, store, nil)
		if err := store.Close(row.ID); err != nil {
			t.Fatalf("closing the session row: %v", err)
		}
		wrote, err := markExecutionResumed(sessionFrontDoor(store), row.ID, "incarnation-1", "work-1", now)
		if wrote || err != nil {
			t.Fatalf("wrote=%v err=%v, want a silent no-op for a closed session", wrote, err)
		}
		after, _ := store.Get(row.ID)
		if got := after.Metadata[executionClaimResumedKey]; got != "" {
			t.Fatalf("%s written on a closed session: %q", executionClaimResumedKey, got)
		}
	})

	t.Run("no session id", func(t *testing.T) {
		wrote, err := markExecutionResumed(sessionFrontDoor(beads.NewMemStore()), "  ", "incarnation-1", "work-1", now)
		if wrote || err != nil {
			t.Fatalf("wrote=%v err=%v, want a silent no-op without a session id", wrote, err)
		}
	})
}
