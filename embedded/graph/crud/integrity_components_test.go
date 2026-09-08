package crud_test

// Exhaustive damage coverage: an object rests on SIX independently stored
// halves (three edges x owner/target side), so there are 63 non-empty ways to
// break it. This walks every one of them against the operations that are
// supposed to leave an object whole.
//
// Two passes, because "can we repair it" depends on where the truth is:
//
//	warm — the process-global object-type cache still remembers the type, so
//	       the type is always recoverable and the WHOLE skeleton must come back;
//	cold — caches purged, so the type must be recovered FROM THE GRAPH: it
//	       survives in B.out (the value of obj.out.to.type) or in C.in (the
//	       type id is encoded in the in-key name). When neither survives the
//	       type is unknowable locally, and the only thing still demanded is
//	       that nothing is left dangling — no half-edge on objects or on the
//	       type vertex.
//
// Edge A never needs the type at all: the objects vertex id is a constant, so
// a lost half of A is always repairable — which is exactly the production case.

import (
	"fmt"
	"strings"

	"github.com/foliagecp/easyjson"
	"github.com/foliagecp/sdk/embedded/graph/crud"
)

type component uint8

const (
	compAOut component = 1 << iota // objects -> obj, owner side
	compAIn                        // objects -> obj, mirror in-key on obj
	compBOut                       // obj -> type, owner side
	compBIn                        // obj -> type, mirror in-key on type
	compCOut                       // type -> obj, owner side
	compCIn                        // type -> obj, mirror in-key on obj
)

var componentNames = []struct {
	bit  component
	name string
}{
	{compAOut, "A.out"}, {compAIn, "A.in"},
	{compBOut, "B.out"}, {compBIn, "B.in"},
	{compCOut, "C.out"}, {compCIn, "C.in"},
}

func maskName(m component) string {
	var parts []string
	for _, c := range componentNames {
		if m&c.bit != 0 {
			parts = append(parts, c.name)
		}
	}
	return strings.Join(parts, "+")
}

// typeRecoverableFromGraph reports whether the object's type can still be
// derived from the graph alone after this damage: B.out holds it as a value,
// C.in holds it inside the in-key name.
func typeRecoverableFromGraph(m component) bool {
	return m&compBOut == 0 || m&compCIn == 0
}

func (s *CMDBClientContractTestSuite) damageComponents(objShort, typeShort string, m component) {
	objID, typeID, objectsID, name := s.ids(objShort, typeShort)
	if m&compAOut != 0 {
		s.dropOutSide(objectsID, name, crud.OBJECT_TYPELINK, objID)
	}
	if m&compAIn != 0 {
		s.dropKey(fmt.Sprintf(crud.InLinkKeyPrefPattern+crud.KeySuff2Pattern, objID, objectsID, name))
	}
	if m&compBOut != 0 {
		s.dropOutSide(objID, "type", crud.TO_TYPELINK, typeID)
	}
	if m&compBIn != 0 {
		s.dropKey(fmt.Sprintf(crud.InLinkKeyPrefPattern+crud.KeySuff2Pattern, typeID, objID, "type"))
	}
	if m&compCOut != 0 {
		s.dropOutSide(typeID, name, crud.OBJECT_TYPELINK, objID)
	}
	if m&compCIn != 0 {
		s.dropKey(fmt.Sprintf(crud.InLinkKeyPrefPattern+crud.KeySuff2Pattern, objID, typeID, name))
	}
}

func (s *CMDBClientContractTestSuite) runComponentMatrix(cold bool) {
	s.bootstrap()
	const objType = "CmType"
	s.NoError(s.dbc.CMDB.TypeCreate(objType))

	c := s.Runtime().Domain.Cache()
	dm := s.Runtime().Domain
	body := easyjson.NewJSONObjectWithKeyValue("v", easyjson.NewJSON(1))

	ops := []struct {
		name     string
		terminal bool
		run      func(objShort string) error
	}{
		{name: "read_v2", run: func(o string) error { _, err := s.dbc.CMDB.ObjectReadV2(o); return err }},
		{name: "update_upsert", run: func(o string) error { return s.dbc.CMDB.ObjectUpdate(o, body, false, objType) }},
		{name: "delete", terminal: true, run: func(o string) error { return s.dbc.CMDB.ObjectDelete(o) }},
	}

	for mask := component(1); mask <= 0x3F; mask++ {
		for oi, op := range ops {
			mask, op := mask, op
			s.Run(fmt.Sprintf("%s/%s", maskName(mask), op.name), func() {
				objShort := fmt.Sprintf("cm-%02x-%d", mask, oi)
				s.Require().NoError(s.dbc.CMDB.ObjectUpdate(objShort, easyjson.NewJSONObject(), false, objType))
				s.Require().Empty(missingIntegrityKeys(c, dm, objShort, objType), "precondition: object must start whole")

				s.damageComponents(objShort, objType, mask)
				if cold {
					// Repair must work off the graph, not off what this
					// process happens to remember.
					crud.ResetPackageCachesForTest()
				}

				_ = op.run(objShort)

				objID := dm.CreateObjectIDWithThisDomain(objShort, false)
				objectsID := dm.CreateObjectIDWithHubDomain(crud.BUILT_IN_OBJECTS, false)
				typeID := dm.CreateObjectIDWithHubDomain(objType, false)
				ctx := fmt.Sprintf("damage=%s op=%s cold=%v", maskName(mask), op.name, cold)

				// Whatever happened, no operation may leave a half-edge behind.
				assertMirrorSymmetry(s.T(), c, objID, ctx+" (object vertex)")
				assertMirrorSymmetry(s.T(), c, objectsID, ctx+" (objects vertex)")
				assertMirrorSymmetry(s.T(), c, typeID, ctx+" (type vertex)")

				if op.terminal {
					return
				}
				if cold && !typeRecoverableFromGraph(mask) {
					// Type unknowable from the graph: edge A is still
					// repairable (the objects id is a constant), the rest is
					// only required not to dangle — checked above.
					return
				}
				assertObjectIntegrity(s.T(), c, dm, objShort, objType,
					ctx+": the object must be whole again")
			})
		}
	}
}

// 63 damages x 3 operations, type recoverable from the process cache.
func (s *CMDBClientContractTestSuite) Test_Integrity_AllComponentSubsets_WarmCache() {
	s.runComponentMatrix(false)
}

// The same, with every auxiliary cache purged: recovery must work off the graph.
func (s *CMDBClientContractTestSuite) Test_Integrity_AllComponentSubsets_ColdCache() {
	s.runComponentMatrix(true)
}
