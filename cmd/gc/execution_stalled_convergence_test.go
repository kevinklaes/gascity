package main

import (
	"bytes"
	"log"
	"strings"
	"testing"
	"time"

	"github.com/gastownhall/gascity/internal/beadmeta"
	"github.com/gastownhall/gascity/internal/beads"
	"github.com/gastownhall/gascity/internal/config"
	sessionpkg "github.com/gastownhall/gascity/internal/session"
	"github.com/gastownhall/gascity/internal/session/sessiontest"
)

// The convergence chain for a claim that never became execution.
//
// This is the liveness half of the execution backstop, and it is load-bearing in
// a way the nudges are not: with the assigned-work wake signal repaired (F3), a
// live session holding an in_progress claim is kept awake BY DESIGN, so the
// no-wake-reason drain that used to recycle wedged seats by accident no longer
// fires for them. If the backstop's exhaustion did not converge, this branch
// would trade an accidental recovery for none at all.
//
// The chain: exhaustion -> TRACKED drain (non-cancelable reason) -> reconciler
// advances it -> runtime stops -> session bead closes -> the dead-assignee
// reopen lane releases the claim -> the row is demand again -> a fresh seat
// claims it.

type stalledConvergenceHarness struct {
	env         *reconcilerTestEnv
	template    string
	sessionID   string
	sessionName string
	work        beads.Bead
	// controllerLog captures the process log, where the drain tracker's scan
	// reports (it has no stdout of its own).
	controllerLog bytes.Buffer
}

func newStalledConvergenceHarness(t *testing.T) *stalledConvergenceHarness {
	t.Helper()
	env := newReconcilerTestEnv()
	template := "worker"
	env.cfg = &config.City{
		Workspace: config.Workspace{Name: "test-city"},
		Agents: []config.Agent{{
			Name:              template,
			StartCommand:      "true",
			Nudge:             "gc hook --claim --drain-ack --json",
			MinActiveSessions: intPtr(0),
			MaxActiveSessions: intPtr(2),
		}},
	}

	manager := sessionpkg.NewManagerWithOptions(env.store, env.sp, sessionpkg.WithClock(env.clk))
	info, err := manager.CreateSession(t.Context(), sessionpkg.CreateOptions{
		BeadOnly: true, Template: template, Title: "pool worker", Command: "true", Provider: "fake",
	})
	if err != nil {
		t.Fatalf("creating the pool session: %v", err)
	}
	h := &stalledConvergenceHarness{env: env, template: template, sessionID: info.ID, sessionName: info.SessionName}
	previousLog := log.Writer()
	log.SetOutput(&h.controllerLog)
	t.Cleanup(func() { log.SetOutput(previousLog) })

	if err := env.store.SetMetadataBatch(info.ID, map[string]string{
		"pool_managed": "true",
		"state":        "active",
	}); err != nil {
		t.Fatalf("marking the session pool-managed: %v", err)
	}
	// Let the reconciler start the seat itself, so the runtime, the session bead
	// and the desired state agree the way they do in production. Starting the
	// provider session behind the reconciler's back leaves a pending create it
	// rolls back ("live runtime belongs to another session").
	env.addDesired(h.sessionName, template, false)
	sessions, err := loadSessionBeads(env.store)
	if err != nil {
		t.Fatalf("loading session beads: %v", err)
	}
	env.reconcile(sessions)
	if !env.sp.IsRunning(h.sessionName) {
		t.Fatalf("the seat did not start; stdout=%s stderr=%s", env.stdout.String(), env.stderr.String())
	}

	work, err := env.store.Create(beads.Bead{
		Title:    "claimed but never executed",
		Type:     "task",
		Metadata: map[string]string{beadmeta.RoutedToMetadataKey: template, beadmeta.RootBeadIDMetadataKey: "root-1"},
	})
	if err != nil {
		t.Fatalf("seeding work: %v", err)
	}
	inProgress := "in_progress"
	if err := env.store.Update(work.ID, beads.UpdateOpts{Status: &inProgress, Assignee: &h.sessionName}); err != nil {
		t.Fatalf("claiming work: %v", err)
	}
	h.work, _ = env.store.Get(work.ID)
	return h
}

func (h *stalledConvergenceHarness) sessionBead(t *testing.T) beads.Bead {
	t.Helper()
	bead, err := h.env.store.Get(h.sessionID)
	if err != nil {
		t.Fatalf("re-reading the session bead: %v", err)
	}
	return bead
}

// drainRequester is the production wiring's shape: a tracked drain with the
// non-cancelable execution-stalled reason.
func (h *stalledConvergenceHarness) drainRequester(t *testing.T) func(beads.Bead) error {
	t.Helper()
	return func(sessionBead beads.Bead) error {
		info := sessiontest.SeedBead(t, sessionBead)
		beginSessionDrainInfo(info, h.env.sp, h.env.dt, executionStalledDrainReason, h.env.clk, defaultDrainTimeout)
		return nil
	}
}

// runBackstopToExhaustion drives the backstop past its grace and every attempt.
func (h *stalledConvergenceHarness) runBackstopToExhaustion(t *testing.T) {
	t.Helper()
	now := h.env.clk.Now()
	for i := 0; i <= idleClaimNudgeMaxAttempts+1; i++ {
		h.env.sp.SetActivity(h.sessionName, now.Add(-time.Hour))
		sessions, err := loadSessionBeads(h.env.store)
		if err != nil {
			t.Fatalf("loading session beads: %v", err)
		}
		work, err := h.env.store.List(beads.ListQuery{Status: "in_progress"})
		if err != nil {
			t.Fatalf("listing work: %v", err)
		}
		stores := make([]beads.Store, len(work))
		refs := make([]string, len(work))
		for j := range work {
			stores[j] = h.env.store
		}
		nudgeStalledPoolExecution(h.env.sp, h.env.cfg, h.env.store, sessions, work, stores, refs, false,
			now, h.env.rec, h.drainRequester(t), &h.env.stdout)
		now = now.Add(idleClaimNudgeGrace + idleClaimNudgeBackoff)
	}
}

// TestExecutionStalledDrainConvergesToAReclaimableRow is the end-to-end chain.
func TestExecutionStalledDrainConvergesToAReclaimableRow(t *testing.T) {
	h := newStalledConvergenceHarness(t)

	h.runBackstopToExhaustion(t)

	ds := h.env.dt.get(h.sessionID)
	if ds == nil {
		t.Fatal("exhaustion did not begin a TRACKED drain; nothing else converges a live seat holding a claim")
	}
	if ds.reason != executionStalledDrainReason {
		t.Fatalf("drain reason = %q, want %q", ds.reason, executionStalledDrainReason)
	}

	// The reconciler advances the tracked drain: deferred interrupt, then stop.
	for i := 0; i < 6 && h.env.sp.IsRunning(h.sessionName); i++ {
		h.env.clk.Advance(defaultDrainTimeout + time.Minute)
		sessions, err := loadSessionBeads(h.env.store)
		if err != nil {
			t.Fatalf("loading session beads: %v", err)
		}
		h.env.reconcile(sessions)
	}
	if h.env.sp.IsRunning(h.sessionName) {
		t.Fatalf("the runtime is still running after the drain advanced; stdout=%s stderr=%s",
			h.env.stdout.String(), h.env.stderr.String())
	}

	// Retire the session bead the way the reconciler's stop path does, then let
	// the dead-assignee reopen lane run: the claim must come back as claimable
	// work rather than staying held by a session that no longer exists.
	if err := h.env.store.Close(h.sessionID); err != nil {
		t.Fatalf("closing the drained session bead: %v", err)
	}
	claimed, err := h.env.store.Get(h.work.ID)
	if err != nil {
		t.Fatalf("re-reading the claim: %v", err)
	}
	released := releaseOrphanedPoolAssignments(h.env.store, beads.SessionStore{Store: h.env.store}, h.env.cfg, "", nil,
		[]beads.Bead{claimed}, []beads.Store{h.env.store}, []string{""}, nil, nil, nil)
	if len(released) != 1 || released[0].ID != h.work.ID {
		t.Fatalf("released = %+v, want the stalled claim reopened", released)
	}

	reopened, err := h.env.store.Get(h.work.ID)
	if err != nil {
		t.Fatalf("re-reading the reopened row: %v", err)
	}
	if !strings.EqualFold(strings.TrimSpace(reopened.Status), "open") || strings.TrimSpace(reopened.Assignee) != "" {
		t.Fatalf("reopened row = status %q assignee %q, want open and unassigned", reopened.Status, reopened.Assignee)
	}

	// And the loop closes: the row is countable demand again, so a fresh seat is
	// minted for it, and a fresh seat's query serves it.
	templates := map[string]struct{}{h.template: {}}
	if _, servable := demandServableForTemplates(h.env.cfg, reopened, templates); !servable {
		t.Fatal("the reopened row is not demand for its template; the chain does not close")
	}
	if !hookClaimMatchesRoute(reopened, hookClaimRouteTargets(h.template)) {
		t.Fatal("the reopened row is not claimable by a worker for its template")
	}
}

// TestExecutionStalledDrainSurvivesTheKeepAliveGuards is the reason this drain
// has its own reason. The session is awake, running, and holding an in_progress
// claim — the exact shape every cancel lens protects — so a cancelable reason
// would be canceled by the very claim that justified draining it, and the seat
// would stay wedged forever.
func TestExecutionStalledDrainSurvivesTheKeepAliveGuards(t *testing.T) {
	h := newStalledConvergenceHarness(t)
	info := sessiontest.SeedBead(t, h.sessionBead(t))
	beginSessionDrainInfo(info, h.env.sp, h.env.dt, executionStalledDrainReason, h.env.clk, defaultDrainTimeout)

	for _, tt := range []struct {
		name   string
		cancel func() bool
	}{
		{"wake reasons reappear", func() bool { return cancelSessionDrainInfo(info, h.env.sp, h.env.dt) }},
		{"pending interaction", func() bool { return cancelSessionDrainForPendingInfo(info, h.env.sp, h.env.dt) }},
		{"assigned work", func() bool { return cancelSessionDrainForAssignedWorkInfo(info, h.env.sp, h.env.dt) }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if tt.cancel() {
				t.Fatalf("%s canceled the execution-stalled drain; the wedged seat would never converge", tt.name)
			}
			if ds := h.env.dt.get(h.sessionID); ds == nil || ds.reason != executionStalledDrainReason {
				t.Fatalf("drain state after %s = %+v, want the execution-stalled drain intact", tt.name, ds)
			}
		})
	}
}

// Control: an ORDINARY drain reason is still cancelable by the same lenses. The
// non-cancelability above is a property of this reason, not a global change to
// how drains behave.
func TestOrdinaryDrainReasonsStayCancelable(t *testing.T) {
	h := newStalledConvergenceHarness(t)
	info := sessiontest.SeedBead(t, h.sessionBead(t))
	beginSessionDrainInfo(info, h.env.sp, h.env.dt, "idle", h.env.clk, defaultDrainTimeout)

	if !cancelSessionDrainInfo(info, h.env.sp, h.env.dt) {
		t.Fatal("an idle drain is no longer cancelable; the non-cancelable reason leaked into the general path")
	}
	if ds := h.env.dt.get(h.sessionID); ds != nil {
		t.Fatalf("idle drain survived cancellation: %+v", ds)
	}
}

// The adversarial case the council named: the agent wakes mid-drain and starts
// executing after the drain was requested. The claim must not be stranded EITHER
// way — the drain proceeds (the seat had its bounded chances), and the work is
// released rather than left held by a session on its way out.
func TestExecutionStalledDrainDoesNotStrandAMidDrainWake(t *testing.T) {
	h := newStalledConvergenceHarness(t)
	h.runBackstopToExhaustion(t)
	if ds := h.env.dt.get(h.sessionID); ds == nil {
		t.Fatal("no tracked drain after exhaustion")
	}

	// The agent wakes up and starts working: fresh activity, and the claim it
	// holds is genuinely in progress.
	h.env.sp.SetActivity(h.sessionName, h.env.clk.Now())
	if err := h.env.store.SetMetadataBatch(h.sessionID, map[string]string{"state": "active"}); err != nil {
		t.Fatalf("marking the session active: %v", err)
	}

	// The drain is not canceled by the wake — it already spent its chances.
	if ds := h.env.dt.get(h.sessionID); ds == nil || ds.reason != executionStalledDrainReason {
		t.Fatalf("the mid-drain wake canceled the drain: %+v", ds)
	}

	// The backstop itself must go quiet now that the session is active: no
	// further nudges, no second drain request.
	before := strings.Count(h.env.stdout.String(), "execution-claim-nudge: nudged")
	drains := 0
	sessions, err := loadSessionBeads(h.env.store)
	if err != nil {
		t.Fatalf("loading session beads: %v", err)
	}
	work, err := h.env.store.List(beads.ListQuery{Status: "in_progress"})
	if err != nil {
		t.Fatalf("listing work: %v", err)
	}
	stores := make([]beads.Store, len(work))
	refs := make([]string, len(work))
	for j := range work {
		stores[j] = h.env.store
	}
	nudgeStalledPoolExecution(h.env.sp, h.env.cfg, h.env.store, sessions, work, stores, refs, false,
		h.env.clk.Now(), h.env.rec, func(beads.Bead) error { drains++; return nil }, &h.env.stdout)

	if got := strings.Count(h.env.stdout.String(), "execution-claim-nudge: nudged"); got != before {
		t.Fatalf("nudges delivered to a now-active session: %d -> %d", before, got)
	}
	if drains != 0 {
		t.Fatalf("second drain requested for an already-draining session (%d); the escalation latch failed", drains)
	}

	// Whichever way the race resolves, the claim ends up actionable: either the
	// agent finishes it, or the session goes away and the reopen lane releases
	// it. What must NOT happen is in_progress work owned by a closed session.
	if err := h.env.store.Close(h.sessionID); err != nil {
		t.Fatalf("closing the session: %v", err)
	}
	claimed, err := h.env.store.Get(h.work.ID)
	if err != nil {
		t.Fatalf("re-reading the claim: %v", err)
	}
	released := releaseOrphanedPoolAssignments(h.env.store, beads.SessionStore{Store: h.env.store}, h.env.cfg, "", nil,
		[]beads.Bead{claimed}, []beads.Store{h.env.store}, []string{""}, nil, nil, nil)
	if len(released) != 1 {
		t.Fatalf("released = %+v, want the claim released once its holder is gone", released)
	}
	reopened, err := h.env.store.Get(h.work.ID)
	if err != nil {
		t.Fatalf("re-reading the row: %v", err)
	}
	if strings.TrimSpace(reopened.Assignee) != "" {
		t.Fatalf("the row is still assigned to a closed session (%q): stranded", reopened.Assignee)
	}
}

// The answer: the one thing that cancels an execution-stalled drain.
//
// The backstop cannot tell a seat inside one long silent command from a seat
// that claimed and went idle. When the first kind comes back and answers the
// claim nudge for the bead it holds, the claim door records that on the session
// row, and the controller cancels the drain while it still can: before the row
// is marked stop-pending and the runtime is killed.

// escalate runs the backstop to exhaustion and returns the latch time, after
// confirming the escalation is the shape an answer can be matched against: a
// tracked execution-stalled drain latched for the row's current incarnation.
func (h *stalledConvergenceHarness) escalate(t *testing.T) time.Time {
	t.Helper()
	h.runBackstopToExhaustion(t)
	ds := h.env.dt.get(h.sessionID)
	if ds == nil || ds.reason != executionStalledDrainReason {
		t.Fatalf("escalation precondition: tracked drain = %+v, want an execution-stalled drain", ds)
	}
	row := h.sessionBead(t)
	token := strings.TrimSpace(row.Metadata["instance_token"])
	if token == "" || strings.TrimSpace(row.Metadata[executionClaimNudgeStalledTokenKey]) != token {
		t.Fatalf("escalation precondition: latch token %q, instance_token %q, want the same non-empty incarnation",
			row.Metadata[executionClaimNudgeStalledTokenKey], token)
	}
	latchedAt, err := time.Parse(time.RFC3339, strings.TrimSpace(row.Metadata[executionClaimNudgeStalledKey]))
	if err != nil {
		t.Fatalf("escalation precondition: latch time %q: %v", row.Metadata[executionClaimNudgeStalledKey], err)
	}
	return latchedAt
}

// answer records the seat's answer through the claim door's own writer, at.
func (h *stalledConvergenceHarness) answer(t *testing.T, at time.Time) {
	t.Helper()
	token := strings.TrimSpace(h.sessionBead(t).Metadata["instance_token"])
	wrote, err := markExecutionResumed(sessionFrontDoor(h.env.store), h.sessionID, token, h.work.ID, at)
	if err != nil || !wrote {
		t.Fatalf("recording the answer: wrote=%v err=%v, want the claim door to write it", wrote, err)
	}
}

// tick runs one reconcile tick, with the provider-backed drain ops the
// controller uses when withDrainOps is set, so the Phase-1 drain-ack arm sees
// the reconciler's own acknowledgement.
func (h *stalledConvergenceHarness) tick(t *testing.T, withDrainOps bool) {
	t.Helper()
	sessions, err := loadSessionBeads(h.env.store)
	if err != nil {
		t.Fatalf("loading session beads: %v", err)
	}
	if !withDrainOps {
		h.env.reconcile(sessions)
		return
	}
	h.env.reconcileWithPoolDesiredAndDrainOps(sessions, map[string]int{h.template: 1}, newDrainOps(h.env.sp))
}

func (h *stalledConvergenceHarness) runtimeMeta(t *testing.T, key string) string {
	t.Helper()
	value, err := h.env.sp.GetMeta(h.sessionName, key)
	if err != nil {
		t.Fatalf("reading runtime meta %s: %v", key, err)
	}
	return value
}

// assertDrainCanceled is the whole observable effect of an honored answer: no
// tracked drain, no drain acknowledgement left for Phase 1 to act on, the
// runtime still running under a row that never went draining, and the
// backstop's state machine reset so a later real stall starts a fresh window.
func (h *stalledConvergenceHarness) assertDrainCanceled(t *testing.T) {
	t.Helper()
	if ds := h.env.dt.get(h.sessionID); ds != nil {
		t.Fatalf("tracked drain after the answer = %+v, want it canceled", ds)
	}
	for _, key := range []string{"GC_DRAIN_ACK", reconcilerDrainAckSourceKey, reconcilerDrainAckReasonKey, reconcilerDrainAckGenerationKey} {
		if got := h.runtimeMeta(t, key); got != "" {
			t.Fatalf("runtime meta %s after the answer = %q, want cleared", key, got)
		}
	}
	if !h.env.sp.IsRunning(h.sessionName) {
		t.Fatalf("the runtime was stopped although the seat answered; stdout=%s stderr=%s",
			h.env.stdout.String(), h.env.stderr.String())
	}
	row := h.sessionBead(t)
	if state := strings.TrimSpace(row.Metadata["state"]); state == string(sessionpkg.StateDraining) {
		t.Fatalf("session state = %q, want the row never marked draining", state)
	}
	if strings.EqualFold(strings.TrimSpace(row.Status), "closed") {
		t.Fatal("the session bead was closed although the seat answered")
	}
	for _, key := range []string{
		executionClaimNudgeWorkKey, executionClaimNudgeCountKey, executionClaimNudgeAtKey,
		executionClaimNudgeStalledKey, executionClaimNudgeStalledTokenKey, executionClaimResumedKey,
	} {
		if got := strings.TrimSpace(row.Metadata[key]); got != "" {
			t.Fatalf("%s after the answer = %q, want the backstop state reset", key, got)
		}
	}
	want := "execution-claim-nudge: " + h.sessionName + " answered for " + h.work.ID + "; canceled execution-stalled drain"
	reported := h.env.stdout.String() + h.controllerLog.String()
	if got := strings.Count(reported, want); got != 1 {
		t.Fatalf("the cancel is reported %d times, want exactly once (%q); output=%s", got, want, reported)
	}
	claim, err := h.env.store.Get(h.work.ID)
	if err != nil {
		t.Fatalf("re-reading the claim: %v", err)
	}
	if !strings.EqualFold(strings.TrimSpace(claim.Status), "in_progress") || strings.TrimSpace(claim.Assignee) != h.sessionName {
		t.Fatalf("claim = status %q assignee %q, want it still held by the answering seat", claim.Status, claim.Assignee)
	}
}

// TestExecutionStalledDrainCanceledWhenTheSeatAnswers: the drain tracker
// honors the answer at either cancelable stage, before the deferred drain
// signal is set and after it. The seat keeps its session and its claim, and
// stays up long after the drain would have timed out.
func TestExecutionStalledDrainCanceledWhenTheSeatAnswers(t *testing.T) {
	t.Run("before the drain signal is set", func(t *testing.T) {
		h := newStalledConvergenceHarness(t)
		latchedAt := h.escalate(t)
		h.answer(t, latchedAt.Add(time.Minute))

		h.tick(t, false)

		h.assertDrainCanceled(t)
	})

	t.Run("after the drain signal is set", func(t *testing.T) {
		h := newStalledConvergenceHarness(t)
		latchedAt := h.escalate(t)
		h.tick(t, false) // the tracker defers the signal one tick, then sets it
		if ds := h.env.dt.get(h.sessionID); ds == nil || !ds.ackSet {
			t.Fatalf("precondition: tracked drain = %+v, want the drain signal set", ds)
		}
		if got := h.runtimeMeta(t, "GC_DRAIN_ACK"); got != "1" {
			t.Fatalf("precondition: GC_DRAIN_ACK = %q, want 1", got)
		}
		h.answer(t, latchedAt.Add(time.Minute))

		h.tick(t, false)

		h.assertDrainCanceled(t)
	})

	t.Run("the seat stays up past the drain timeout", func(t *testing.T) {
		h := newStalledConvergenceHarness(t)
		latchedAt := h.escalate(t)
		h.answer(t, latchedAt.Add(time.Minute))
		h.tick(t, false)

		for i := 0; i < 3; i++ {
			h.env.clk.Advance(defaultDrainTimeout + time.Minute)
			h.tick(t, false)
		}

		h.assertDrainCanceled(t)
	})
}

// TestExecutionStalledAckCanceledInPhaseOneWhenTheSeatAnswers: the last
// cancelable moment. The drain signal is set and the Phase-1 drain-ack arm is
// about to mark the row stop-pending and kill the runtime; an answer that
// landed in between cancels the drain instead, and nothing is written to the
// row's lifecycle.
func TestExecutionStalledAckCanceledInPhaseOneWhenTheSeatAnswers(t *testing.T) {
	t.Run("tracked drain", func(t *testing.T) {
		h := newStalledConvergenceHarness(t)
		latchedAt := h.escalate(t)
		h.tick(t, true) // sets the drain signal; Phase 1 has not seen it yet
		if got := h.runtimeMeta(t, "GC_DRAIN_ACK"); got != "1" {
			t.Fatalf("precondition: GC_DRAIN_ACK = %q, want 1", got)
		}
		h.answer(t, latchedAt.Add(time.Minute))

		h.tick(t, true)

		h.assertDrainCanceled(t)
		if got := strings.TrimSpace(h.sessionBead(t).Metadata["state_reason"]); got == "drain-ack-stop-pending" {
			t.Fatalf("state_reason = %q, want no stop-pending write", got)
		}
	})

	t.Run("the tracker entry did not survive a controller restart", func(t *testing.T) {
		h := newStalledConvergenceHarness(t)
		latchedAt := h.escalate(t)
		h.tick(t, true)
		h.env.dt = newDrainTracker() // the in-memory drain is gone; the signal on the runtime is not
		h.answer(t, latchedAt.Add(time.Minute))

		h.tick(t, true)

		h.assertDrainCanceled(t)
	})
}

// TestExecutionStalledDrainIgnoresAStaleAnswer: only the escalated incarnation
// answering AFTER the escalation cancels the drain. An answer older than the
// latch (it answered an earlier escalation) or a latch that belongs to another
// incarnation leaves the drain exactly as it was: it reaches stop-pending and
// the runtime is stopped.
func TestExecutionStalledDrainIgnoresAStaleAnswer(t *testing.T) {
	for _, tc := range []struct {
		name  string
		stale func(latchedAt time.Time) map[string]string
	}{
		{"an answer older than the latch", func(latchedAt time.Time) map[string]string {
			return map[string]string{executionClaimResumedKey: latchedAt.Add(-time.Second).UTC().Format(time.RFC3339)}
		}},
		{"a latch for another incarnation", func(latchedAt time.Time) map[string]string {
			return map[string]string{
				executionClaimNudgeStalledTokenKey: "an-earlier-incarnation",
				executionClaimResumedKey:           latchedAt.Add(time.Minute).UTC().Format(time.RFC3339),
			}
		}},
		{"an unreadable answer", func(time.Time) map[string]string {
			return map[string]string{executionClaimResumedKey: "soon"}
		}},
		{"no answer", func(time.Time) map[string]string { return nil }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newStalledConvergenceHarness(t)
			latchedAt := h.escalate(t)
			if meta := tc.stale(latchedAt); meta != nil {
				if err := h.env.store.SetMetadataBatch(h.sessionID, meta); err != nil {
					t.Fatalf("seeding the stale answer: %v", err)
				}
			}

			h.tick(t, true) // sets the drain signal
			if ds := h.env.dt.get(h.sessionID); ds == nil || !ds.ackSet {
				t.Fatalf("tracked drain after the first tick = %+v, want it intact with the signal set", ds)
			}
			h.tick(t, true) // Phase 1 honors the signal

			row := h.sessionBead(t)
			if state := strings.TrimSpace(row.Metadata["state"]); state != string(sessionpkg.StateDraining) {
				t.Fatalf("session state = %q, want %q: a stale answer must not cancel the drain; stdout=%s stderr=%s",
					state, sessionpkg.StateDraining, h.env.stdout.String(), h.env.stderr.String())
			}
			if reported := h.env.stdout.String() + h.controllerLog.String(); strings.Contains(reported, "canceled execution-stalled drain") {
				t.Fatalf("a cancel is reported for a stale answer: %s", reported)
			}
			if strings.TrimSpace(row.Metadata[executionClaimNudgeStalledKey]) == "" {
				t.Fatal("the escalation latch was cleared although the drain was not canceled")
			}
		})
	}
}
