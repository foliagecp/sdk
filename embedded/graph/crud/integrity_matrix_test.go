package crud_test

// THE INTEGRITY MATRIX: every way the CMDB skeleton can lose half an edge,
// crossed with every CRUD operation that touches the object.
//
// Contract asserted here (the DESIRED behaviour, not today's): any CRUD
// operation on an object leaves that object structurally whole. A read that
// can see the damage can repair it; an update that rewrites the object can
// repair it; a delete must not leave half-edges behind.
//
// Born from a production stand where a live objects->obj out-side had lost its
// mirror in-key inside the runtime cache: object.read answered
// "not an object, not connected to objects topology" forever, and no CRUD path
// healed it — UpdateObject's orphan repair only runs when __type cannot be
// resolved, which was not the case there.
//
// Red cells are the map of what is still missing. Cells that stay red after
// the root-cause fix are the ones that need explicit self-healing.

import (
	"fmt"

	"github.com/foliagecp/easyjson"
	"github.com/foliagecp/sdk/embedded/graph/crud"
	"github.com/foliagecp/sdk/statefun/system"
)

// ---------------------------------------------------------------- damages

// damage removes exactly one half of one edge of the object skeleton.
type damage struct {
	name  string
	apply func(s *CMDBClientContractTestSuite, objShort, typeShort string)
}

func (s *CMDBClientContractTestSuite) dropKey(key string) {
	s.Runtime().Domain.Cache().DeleteValue(key, true, system.GetCurrentTimeNs())
}

// dropOutSide removes the complete owner side of one link (to + body + ltype),
// leaving the mirror in-key on the target dangling.
func (s *CMDBClientContractTestSuite) dropOutSide(from, linkName, linkType, to string) {
	s.dropKey(fmt.Sprintf(crud.OutLinkTargetKeyPrefPattern+crud.KeySuff1Pattern, from, linkName))
	s.dropKey(fmt.Sprintf(crud.OutLinkBodyKeyPrefPattern+crud.KeySuff1Pattern, from, linkName))
	s.dropKey(fmt.Sprintf(crud.OutLinkTypeKeyPrefPattern+crud.KeySuff2Pattern, from, linkType, to))
}

func (s *CMDBClientContractTestSuite) ids(objShort, typeShort string) (objID, typeID, objectsID, name string) {
	dm := s.Runtime().Domain
	objID = dm.CreateObjectIDWithThisDomain(objShort, false)
	typeID = dm.CreateObjectIDWithHubDomain(typeShort, false)
	objectsID = dm.CreateObjectIDWithHubDomain(crud.BUILT_IN_OBJECTS, false)
	name = dm.GetObjectIDWithoutDomain(objID)
	return
}

func integrityDamages() []damage {
	return []damage{
		{ // the production stand case
			name: "D1_objects_in_key_missing",
			apply: func(s *CMDBClientContractTestSuite, o, t string) {
				objID, _, objectsID, name := s.ids(o, t)
				s.dropKey(fmt.Sprintf(crud.InLinkKeyPrefPattern+crud.KeySuff2Pattern, objID, objectsID, name))
			},
		},
		{
			name: "D2_objects_out_side_missing",
			apply: func(s *CMDBClientContractTestSuite, o, t string) {
				objID, _, objectsID, name := s.ids(o, t)
				s.dropOutSide(objectsID, name, crud.OBJECT_TYPELINK, objID)
			},
		},
		{
			name: "D3_type_in_key_missing",
			apply: func(s *CMDBClientContractTestSuite, o, t string) {
				objID, typeID, _, _ := s.ids(o, t)
				s.dropKey(fmt.Sprintf(crud.InLinkKeyPrefPattern+crud.KeySuff2Pattern, typeID, objID, "type"))
			},
		},
		{
			name: "D4_type_out_side_missing",
			apply: func(s *CMDBClientContractTestSuite, o, t string) {
				objID, typeID, _, name := s.ids(o, t)
				s.dropOutSide(typeID, name, crud.OBJECT_TYPELINK, objID)
			},
		},
		{ // orphan: __type unresolvable — the one class repair covers today
			name: "D5_obj_type_out_side_missing",
			apply: func(s *CMDBClientContractTestSuite, o, t string) {
				objID, typeID, _, _ := s.ids(o, t)
				s.dropOutSide(objID, "type", crud.TO_TYPELINK, typeID)
			},
		},
		{
			name: "D6_obj_in_key_from_type_missing",
			apply: func(s *CMDBClientContractTestSuite, o, t string) {
				objID, typeID, _, name := s.ids(o, t)
				s.dropKey(fmt.Sprintf(crud.InLinkKeyPrefPattern+crud.KeySuff2Pattern, objID, typeID, name))
			},
		},
		{ // index lost while the link itself lives
			name: "D7_objects_ltype_index_missing",
			apply: func(s *CMDBClientContractTestSuite, o, t string) {
				objID, _, objectsID, _ := s.ids(o, t)
				s.dropKey(fmt.Sprintf(crud.OutLinkTypeKeyPrefPattern+crud.KeySuff2Pattern, objectsID, crud.OBJECT_TYPELINK, objID))
			},
		},
	}
}

// -------------------------------------------------------------- operations

// operation is a CRUD call on the damaged object. terminal marks operations
// that intentionally remove the object from the model (delete): those are held
// to the half-edge contract instead of the object skeleton.
type operation struct {
	name     string
	terminal bool
	run      func(s *CMDBClientContractTestSuite, objShort, typeShort, partnerShort string) error
}

func integrityOperations() []operation {
	body := easyjson.NewJSONObjectWithKeyValue("v", easyjson.NewJSON(1))
	return []operation{
		{name: "O1_object_read", run: func(s *CMDBClientContractTestSuite, o, t, p string) error {
			_, err := s.dbc.CMDB.ObjectRead(o)
			return err
		}},
		{name: "O2_object_read_v2", run: func(s *CMDBClientContractTestSuite, o, t, p string) error {
			_, err := s.dbc.CMDB.ObjectReadV2(o)
			return err
		}},
		{name: "O3_object_update_upsert", run: func(s *CMDBClientContractTestSuite, o, t, p string) error {
			return s.dbc.CMDB.ObjectUpdate(o, body, false, t)
		}},
		{name: "O4_object_update_plain", run: func(s *CMDBClientContractTestSuite, o, t, p string) error {
			return s.dbc.CMDB.ObjectUpdate(o, body, false)
		}},
		{name: "O5_object_create_again", run: func(s *CMDBClientContractTestSuite, o, t, p string) error {
			return s.dbc.CMDB.ObjectCreate(o, t, body)
		}},
		{name: "O6_objects_link_create", run: func(s *CMDBClientContractTestSuite, o, t, p string) error {
			return s.dbc.CMDB.ObjectsLinkCreate(o, p, "im-edge", nil)
		}},
		{name: "O7_objects_link_read", run: func(s *CMDBClientContractTestSuite, o, t, p string) error {
			_, err := s.dbc.CMDB.ObjectsLinkRead(o, p)
			return err
		}},
		{name: "O8_object_delete", terminal: true, run: func(s *CMDBClientContractTestSuite, o, t, p string) error {
			return s.dbc.CMDB.ObjectDelete(o)
		}},
	}
}

// ------------------------------------------------------------------ matrix

func (s *CMDBClientContractTestSuite) Test_Integrity_Matrix_DamageTimesOperation() {
	s.bootstrap()
	const objType, partnerType = "ImType", "ImPartner"
	s.NoError(s.dbc.CMDB.TypeCreate(objType))
	s.NoError(s.dbc.CMDB.TypeCreate(partnerType))
	s.NoError(s.dbc.CMDB.TypesLinkCreate(objType, partnerType, "im-rel", nil))

	c := s.Runtime().Domain.Cache()
	dm := s.Runtime().Domain

	for di, d := range integrityDamages() {
		for oi, op := range integrityOperations() {
			d, op := d, op
			s.Run(d.name+"/"+op.name, func() {
				objShort := fmt.Sprintf("im-o%d-%d", di, oi)
				partnerShort := fmt.Sprintf("im-p%d-%d", di, oi)

				// A healthy object (+ a partner to link to), verified whole.
				s.Require().NoError(s.dbc.CMDB.ObjectUpdate(objShort, easyjson.NewJSONObject(), false, objType))
				s.Require().NoError(s.dbc.CMDB.ObjectUpdate(partnerShort, easyjson.NewJSONObject(), false, partnerType))
				s.Require().Empty(missingIntegrityKeys(c, dm, objShort, objType), "precondition: object must start whole")

				d.apply(s, objShort, objType)

				// The operation may legitimately report an error while the
				// object is broken; what it must NOT do is leave it broken.
				_ = op.run(s, objShort, objType, partnerShort)

				objID := dm.CreateObjectIDWithThisDomain(objShort, false)
				if op.terminal {
					// Delete: the object leaves the model, but no half-edges
					// may be left behind on the vertices that referenced it.
					assertMirrorSymmetry(s.T(), c, dm.CreateObjectIDWithHubDomain(crud.BUILT_IN_OBJECTS, false),
						"after "+op.name+" on "+d.name+": objects vertex")
					assertMirrorSymmetry(s.T(), c, dm.CreateObjectIDWithHubDomain(objType, false),
						"after "+op.name+" on "+d.name+": type vertex")
					assertMirrorSymmetry(s.T(), c, objID,
						"after "+op.name+" on "+d.name+": deleted object vertex")
					return
				}

				assertObjectIntegrity(s.T(), c, dm, objShort, objType,
					"after "+op.name+" the object damaged by "+d.name+" must be whole again")
				assertMirrorSymmetry(s.T(), c, objID, "after "+op.name+" on "+d.name)
			})
		}
	}
}

// Sanity: a freshly created object satisfies the full twelve-key skeleton and
// the mirror invariant. If this fails, the invariant itself is wrong and every
// other integrity test is meaningless.
func (s *CMDBClientContractTestSuite) Test_Integrity_HealthyObjectSatisfiesInvariant() {
	s.bootstrap()
	s.NoError(s.dbc.CMDB.TypeCreate("IntTypeA"))
	s.NoError(s.dbc.CMDB.ObjectUpdate("int-ok", easyjson.NewJSONObject(), false, "IntTypeA"))

	c := s.Runtime().Domain.Cache()
	dm := s.Runtime().Domain
	assertObjectIntegrity(s.T(), c, dm, "int-ok", "IntTypeA", "a freshly created object must be whole")
	assertMirrorSymmetry(s.T(), c, dm.CreateObjectIDWithThisDomain("int-ok", false), "fresh object")
	assertMirrorSymmetry(s.T(), c, dm.CreateObjectIDWithHubDomain("IntTypeA", false), "fresh type")
}
