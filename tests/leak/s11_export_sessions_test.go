//go:build leak

package leak

import (
	"fmt"
	"testing"
	"time"

	"github.com/foliagecp/easyjson"
	"github.com/foliagecp/sdk/embedded/graph/debug"
	sfMediators "github.com/foliagecp/sdk/statefun/mediator"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
	"github.com/stretchr/testify/suite"
)

// S11 — chunked graph-export sessions: the in-memory session store holds the
// whole serialized export until finish_session or the TTL sweep. The cycle
// mixes completed exports (explicit finish) with abandoned ones (client
// vanished mid-poll) and requires the store to drain to zero once the TTL
// (shrunk to 2s here) has passed. Expected: PASS.

const printGraphTypename = "functions.graph.api.object.debug.print.graph"

type S11Suite struct{ leakSuite }

func TestS11ExportSessions(t *testing.T) { suite.Run(t, new(S11Suite)) }

func (s *S11Suite) exportRequest(id string, payload easyjson.JSON) (easyjson.JSON, error) {
	reply, err := s.Request(sfPlugins.AutoRequestSelect, printGraphTypename, id, &payload, nil)
	om := sfMediators.OpMsgFromSfReply(reply, err)
	if om.Status != sfMediators.SYNC_OP_STATUS_OK {
		return om.Data, fmt.Errorf("export request failed: %s (%s)", sfMediators.OpStatusNames[om.Status], om.Details)
	}
	return om.Data, nil
}

func (s *S11Suite) startChunkedExport(root string) (sessionID string, totalChunks int, err error) {
	p := easyjson.NewJSONObject()
	p.SetByPath("format", easyjson.NewJSON("dot"))
	p.SetByPath("delivery", easyjson.NewJSON("chunks"))
	data, err := s.exportRequest(root, p)
	if err != nil {
		return "", 0, err
	}
	return data.GetByPath("session_id").AsStringDefault(""),
		int(data.GetByPath("total_chunks").AsNumericDefault(0)), nil
}

func (s *S11Suite) getChunk(root, sessionID string, index int) error {
	p := easyjson.NewJSONObject()
	p.SetByPath("export_action", easyjson.NewJSON("get_chunk"))
	p.SetByPath("session_id", easyjson.NewJSON(sessionID))
	p.SetByPath("chunk_index", easyjson.NewJSON(index))
	_, err := s.exportRequest(root, p)
	return err
}

func (s *S11Suite) finishSession(root, sessionID string) error {
	p := easyjson.NewJSONObject()
	p.SetByPath("export_action", easyjson.NewJSON("finish_session"))
	p.SetByPath("session_id", easyjson.NewJSON(sessionID))
	_, err := s.exportRequest(root, p)
	return err
}

func (s *S11Suite) Test_ExportSessionChurn() {
	debug.SetSessionTTLForTest(2 * time.Second)
	s.bootCRUD(debug.RegisterAllFunctionTypes)
	root := s.buildStaticQueryGraph("s11", 5)
	k := scaled(5)

	cycle := func(c int) error {
		// Completed exports: fetch every chunk, then release explicitly.
		for i := 0; i < k; i++ {
			sid, chunks, err := s.startChunkedExport(root)
			if err != nil {
				return err
			}
			if sid == "" || chunks < 1 {
				return fmt.Errorf("bad chunked export reply: session=%q chunks=%d", sid, chunks)
			}
			for j := 0; j < chunks; j++ {
				if err := s.getChunk(root, sid, j); err != nil {
					return err
				}
			}
			if err := s.finishSession(root, sid); err != nil {
				return err
			}
		}
		// Abandoned exports: one chunk fetched, never finished — must die by
		// TTL alone.
		for i := 0; i < k; i++ {
			sid, _, err := s.startChunkedExport(root)
			if err != nil {
				return err
			}
			if err := s.getChunk(root, sid, 0); err != nil {
				return err
			}
		}
		// Wait out the TTL and require the store to drain to zero.
		deadline := time.Now().Add(15 * time.Second)
		for {
			debug.SweepExpiredForTest()
			if debug.SessionCountForTest() == 0 {
				return nil
			}
			if time.Now().After(deadline) {
				return fmt.Errorf("%d export sessions still alive after TTL", debug.SessionCountForTest())
			}
			time.Sleep(200 * time.Millisecond)
		}
	}

	collect := func(smp *Sample) {
		s.collectCore(smp)
		smp.Custom["export_sessions"] = float64(debug.SessionCountForTest())
	}
	rep := s.newRunner("s11_export_sessions", cycle, collect).Run(s.T())
	rep.AssertClean(s.T())
	s.assertCoreStable(rep)
	rep.AssertStable(s.T(), "export_sessions")
}
