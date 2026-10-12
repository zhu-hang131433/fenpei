package server

import (
	"bytes"
	"fenpei/internal/protocol"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

func addTestSource(t *testing.T, s *Server, root string) string {
	t.Helper()
	if err := os.MkdirAll(filepath.Join(root, "task"), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "task", "data"), []byte("data"), 0644); err != nil {
		t.Fatal(err)
	}
	result, err := s.addSource(root, "")
	if err != nil {
		t.Fatal(err)
	}
	return result.SourceID
}
func TestRemoveLastTaskAllowsParentImport(t *testing.T) {
	s := New("")
	parent := t.TempDir()
	root := filepath.Join(parent, "child")
	id := addTestSource(t, s, root)
	req := httptest.NewRequest("POST", "/api/remove", bytes.NewBufferString(`{"folder":"`+id+`/task"}`))
	res := httptest.NewRecorder()
	s.handleRemoveFolder(res, req)
	if res.Code != 200 {
		t.Fatalf("remove: %s", res.Body.String())
	}
	if _, ok := s.st.Sources[id]; ok {
		t.Fatal("empty source retained")
	}
	if _, err := s.addSource(parent, ""); err != nil {
		t.Fatal(err)
	}
}
func TestEmptyLegacySourceDoesNotBlockButLiveSourceDoes(t *testing.T) {
	s := New("")
	parent := t.TempDir()
	root := filepath.Join(parent, "child")
	id := addTestSource(t, s, root)
	if err := s.validateRootLocked(parent); err == nil {
		t.Fatal("live overlap allowed")
	}
	delete(s.st.Folders, id+"/task")
	if err := s.validateRootLocked(parent); err != nil {
		t.Fatal(err)
	}
}
func TestDeviceChangeRejectedWithoutChangingState(t *testing.T) {
	s := New("")
	root := t.TempDir()
	id := addTestSource(t, s, root)
	s.st.Sources[id].Device = deviceOf(root) + 1
	device := s.st.Sources[id].Device
	if _, err := s.addSource(root, ""); err == nil {
		t.Fatal("device change accepted")
	}
	if s.st.Sources[id].Device != device || len(s.st.Folders) != 1 {
		t.Fatal("state mutated")
	}
}
func TestCompletionRequiresCurrentClaimAndIsIdempotent(t *testing.T) {
	s := New("")
	id := addTestSource(t, s, t.TempDir())
	key := id + "/task"
	a, _ := s.register("a", "a")
	b, _ := s.register("b", "b")
	_, f := s.next(a)
	old := f.ClaimToken
	// Simulate the same task being reassigned after a timeout.
	f.Status = protocol.StatusPending
	f.ClaimedBy = nil
	_, f = s.next(b)
	current := f.ClaimToken
	if ok, _ := s.complete(a, key, old); ok {
		t.Fatal("stale owner accepted")
	}
	if ok, _ := s.complete(b, key, old); ok {
		t.Fatal("stale token accepted")
	}
	if ok, _ := s.complete(b, key, ""); ok {
		t.Fatal("missing token accepted")
	}
	for i := 0; i < 2; i++ {
		if ok, msg := s.complete(b, key, current); !ok {
			t.Fatal(msg)
		}
	}
	if len(s.st.Workers[b].CompletedFolders) != 1 {
		t.Fatal("duplicate completion counted")
	}
}
func TestPersistenceFailureVisibleAndRecoverySavesState(t *testing.T) {
	s := New("")
	root := t.TempDir()
	s.stateFile = filepath.Join(root, "missing", "state.json")
	s.persist()
	if s.snapshot().PersistenceError == "" {
		t.Fatal("failure hidden")
	}
	if err := os.Mkdir(filepath.Dir(s.stateFile), 0755); err != nil {
		t.Fatal(err)
	}
	s.persist()
	if s.snapshot().PersistenceError != "" {
		t.Fatal("error not cleared")
	}
	if _, err := os.ReadFile(s.stateFile); err != nil {
		t.Fatal(err)
	}
	// Rename failure must preserve the destination and leave no temporary files.
	if err := writeStateFile(root, []byte("bad")); err == nil {
		t.Fatal("rename failure ignored")
	}
	files, _ := filepath.Glob(filepath.Join(root, ".fenpei-state-*"))
	if len(files) != 0 {
		t.Fatal("temporary files leaked")
	}
}

func TestReclaimBySameWorkerInvalidatesPreviousToken(t *testing.T) {
	s := New("")
	id := addTestSource(t, s, t.TempDir())
	worker, _ := s.register("local", "worker")
	_, first := s.next(worker)
	old := first.ClaimToken
	_, second := s.next(worker)
	if old == second.ClaimToken {
		t.Fatal("claim token reused")
	}
	if ok, _ := s.complete(worker, id+"/task", old); ok {
		t.Fatal("old claim accepted")
	}
	if ok, msg := s.complete(worker, id+"/task", second.ClaimToken); !ok {
		t.Fatal(msg)
	}
}
