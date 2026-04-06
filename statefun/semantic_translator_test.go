package statefun

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- mock SemanticHandler ---

type mockHandler struct {
	typePuts        []typePutCall
	typeDeletes     []string
	objectPuts      []objectPutCall
	objectDeletes   []string
	objectLinkPuts  []objectLinkPutCall
	objectLinkDels  []objectLinkDelCall
	typeLinkPuts    []typeLinkPutCall
	typeLinkDels    []typeLinkDelCall
	returnErr       error
}

type typePutCall struct {
	id   string
	body string
}
type objectPutCall struct {
	id, typeID string
	body       string
}
type objectLinkPutCall struct {
	from, to, name, linkType string
	body                     string
	tags                     []string
}
type objectLinkDelCall struct {
	from, name string
}
type typeLinkPutCall struct {
	from, to, name, linkType string
	body                     string
	tags                     []string
}
type typeLinkDelCall struct {
	from, name string
}

func (m *mockHandler) OnTypePut(id string, body json.RawMessage) error {
	m.typePuts = append(m.typePuts, typePutCall{id, string(body)})
	return m.returnErr
}
func (m *mockHandler) OnTypeDelete(id string) error {
	m.typeDeletes = append(m.typeDeletes, id)
	return m.returnErr
}
func (m *mockHandler) OnObjectPut(id, typeID string, body json.RawMessage) error {
	m.objectPuts = append(m.objectPuts, objectPutCall{id, typeID, string(body)})
	return m.returnErr
}
func (m *mockHandler) OnObjectDelete(id string) error {
	m.objectDeletes = append(m.objectDeletes, id)
	return m.returnErr
}
func (m *mockHandler) OnObjectLinkPut(from, to, name, linkType string, body json.RawMessage, tags []string) error {
	m.objectLinkPuts = append(m.objectLinkPuts, objectLinkPutCall{from, to, name, linkType, string(body), tags})
	return m.returnErr
}
func (m *mockHandler) OnObjectLinkDelete(from, name string) error {
	m.objectLinkDels = append(m.objectLinkDels, objectLinkDelCall{from, name})
	return m.returnErr
}
func (m *mockHandler) OnTypeLinkPut(from, to, name, linkType string, body json.RawMessage, tags []string) error {
	m.typeLinkPuts = append(m.typeLinkPuts, typeLinkPutCall{from, to, name, linkType, string(body), tags})
	return m.returnErr
}
func (m *mockHandler) OnTypeLinkDelete(from, name string) error {
	m.typeLinkDels = append(m.typeLinkDels, typeLinkDelCall{from, name})
	return m.returnErr
}

// --- helpers ---

func makeEvent(ops ...ExportOp) ExportEvent {
	return ExportEvent{TxID: "test-tx", Domain: "hub", Ops: ops}
}

func vertexPut(id, bodyJSON string) ExportOp {
	return ExportOp{Op: "vertex_put", ID: id, Body: json.RawMessage(bodyJSON)}
}

func vertexDelete(id string) ExportOp {
	return ExportOp{Op: "vertex_delete", ID: id}
}

func linkPut(from, to, name, linkType, bodyJSON string) ExportOp {
	return ExportOp{Op: "link_put", From: from, To: to, Name: name, LinkType: linkType, Body: json.RawMessage(bodyJSON)}
}

func linkDelete(from, name string) ExportOp {
	return ExportOp{Op: "link_delete", From: from, Name: name}
}

func linkDeleteWithType(from, to, name, linkType string) ExportOp {
	return ExportOp{Op: "link_delete", From: from, To: to, Name: name, LinkType: linkType}
}

// --- tests ---

// TestSemanticTranslator_HubNotClassified verifies that topology hub vertices
// (hub/types, hub/objects) are never classified as types or objects.
func TestSemanticTranslator_HubNotClassified(t *testing.T) {
	h := &mockHandler{}
	tr := NewSemanticTranslator(h)

	// Structural links identify the hubs but must not classify them.
	err := tr.ProcessEvent(makeEvent(
		linkPut("hub/root", "hub/types", "hub/types", "__types", "{}"),
		linkPut("hub/root", "hub/objects", "hub/objects", "__objects", "{}"),
		vertexPut("hub/types", "{}"),
		vertexPut("hub/objects", "{}"),
	))
	require.NoError(t, err)

	assert.Empty(t, h.typePuts, "hub/types must not be dispatched as a type")
	assert.Empty(t, h.objectPuts, "hub/objects must not be dispatched as an object")
}

// TestSemanticTranslator_TypeClassification verifies that __type links FROM the
// types-hub classify the target vertex as a type.
func TestSemanticTranslator_TypeClassification(t *testing.T) {
	h := &mockHandler{}
	tr := NewSemanticTranslator(h)

	err := tr.ProcessEvent(makeEvent(
		// Structural link identifies hub/types as the types-hub.
		linkPut("hub/root", "hub/types", "hub/types", "__types", "{}"),
		// Classification links: hub/types → actual types (via __type).
		linkPut("hub/types", "hub/server", "hub/server", "__type", "{}"),
		linkPut("hub/types", "hub/rack", "hub/rack", "__type", "{}"),
		// Vertex bodies.
		vertexPut("hub/server", `{"desc":"server type"}`),
		vertexPut("hub/rack", `{"desc":"rack type"}`),
	))
	require.NoError(t, err)

	require.Len(t, h.typePuts, 2)
	assert.Equal(t, "hub/server", h.typePuts[0].id)
	assert.Contains(t, h.typePuts[0].body, "server type")
	assert.Equal(t, "hub/rack", h.typePuts[1].id)
}

// TestSemanticTranslator_ObjectClassification verifies that __object links FROM the
// objects-hub classify the target vertex as an object.
func TestSemanticTranslator_ObjectClassification(t *testing.T) {
	h := &mockHandler{}
	tr := NewSemanticTranslator(h)

	err := tr.ProcessEvent(makeEvent(
		linkPut("hub/root", "hub/types", "hub/types", "__types", "{}"),
		linkPut("hub/root", "hub/objects", "hub/objects", "__objects", "{}"),
		linkPut("hub/types", "hub/server", "hub/server", "__type", "{}"),
		linkPut("hub/objects", "hub/srv-0", "hub/srv-0", "__object", "{}"),
		// Object's type association.
		linkPut("hub/srv-0", "hub/server", "type", "__type", "{}"),
		vertexPut("hub/srv-0", `{"ip":"10.0.0.1"}`),
	))
	require.NoError(t, err)

	require.Len(t, h.objectPuts, 1)
	assert.Equal(t, "hub/srv-0", h.objectPuts[0].id)
	assert.Equal(t, "hub/server", h.objectPuts[0].typeID)
	assert.Contains(t, h.objectPuts[0].body, "10.0.0.1")
}

// TestSemanticTranslator_ObjectTypeAssociation verifies that the objectTypes map
// is populated from __type links where From is a classified object.
func TestSemanticTranslator_ObjectTypeAssociation(t *testing.T) {
	h := &mockHandler{}
	tr := NewSemanticTranslator(h)

	err := tr.ProcessEvent(makeEvent(
		linkPut("hub/root", "hub/types", "hub/types", "__types", "{}"),
		linkPut("hub/root", "hub/objects", "hub/objects", "__objects", "{}"),
		linkPut("hub/types", "hub/device", "hub/device", "__type", "{}"),
		linkPut("hub/objects", "hub/dev-1", "hub/dev-1", "__object", "{}"),
		linkPut("hub/dev-1", "hub/device", "type", "__type", "{}"),
		vertexPut("hub/dev-1", `{"hostname":"dev-1"}`),
		vertexPut("hub/device", "{}"),
	))
	require.NoError(t, err)

	require.Len(t, h.objectPuts, 1)
	assert.Equal(t, "hub/device", h.objectPuts[0].typeID)
}

// TestSemanticTranslator_TypeLinkDispatch verifies that __type links between two
// classified type vertices are dispatched as OnTypeLinkPut.
func TestSemanticTranslator_TypeLinkDispatch(t *testing.T) {
	h := &mockHandler{}
	tr := NewSemanticTranslator(h)

	err := tr.ProcessEvent(makeEvent(
		linkPut("hub/root", "hub/types", "hub/types", "__types", "{}"),
		linkPut("hub/types", "hub/server", "hub/server", "__type", "{}"),
		linkPut("hub/types", "hub/rack", "hub/rack", "__type", "{}"),
		// Type-to-type relationship link.
		linkPut("hub/server", "hub/rack", "hub/rack", "__type", `{"type":"hosted_in"}`),
		vertexPut("hub/server", "{}"),
		vertexPut("hub/rack", "{}"),
	))
	require.NoError(t, err)

	require.Len(t, h.typeLinkPuts, 1)
	assert.Equal(t, "hub/server", h.typeLinkPuts[0].from)
	assert.Equal(t, "hub/rack", h.typeLinkPuts[0].to)
	assert.Equal(t, "hub/rack", h.typeLinkPuts[0].name)
	assert.Equal(t, "hosted_in", h.typeLinkPuts[0].linkType)
}

// TestSemanticTranslator_ObjectLinkDispatch verifies that regular links between
// two classified objects are dispatched as OnObjectLinkPut.
func TestSemanticTranslator_ObjectLinkDispatch(t *testing.T) {
	h := &mockHandler{}
	tr := NewSemanticTranslator(h)

	err := tr.ProcessEvent(makeEvent(
		linkPut("hub/root", "hub/types", "hub/types", "__types", "{}"),
		linkPut("hub/root", "hub/objects", "hub/objects", "__objects", "{}"),
		linkPut("hub/types", "hub/server", "hub/server", "__type", "{}"),
		linkPut("hub/types", "hub/rack", "hub/rack", "__type", "{}"),
		linkPut("hub/objects", "hub/srv-0", "hub/srv-0", "__object", "{}"),
		linkPut("hub/objects", "hub/rack-A", "hub/rack-A", "__object", "{}"),
		linkPut("hub/srv-0", "hub/server", "type", "__type", "{}"),
		linkPut("hub/rack-A", "hub/rack", "type", "__type", "{}"),
		// Object-to-object relationship link.
		linkPut("hub/srv-0", "hub/rack-A", "rack-link-0", "hosted_in", "{}"),
		vertexPut("hub/srv-0", "{}"),
		vertexPut("hub/rack-A", "{}"),
	))
	require.NoError(t, err)

	require.Len(t, h.objectLinkPuts, 1)
	assert.Equal(t, "hub/srv-0", h.objectLinkPuts[0].from)
	assert.Equal(t, "hub/rack-A", h.objectLinkPuts[0].to)
	assert.Equal(t, "rack-link-0", h.objectLinkPuts[0].name)
	assert.Equal(t, "hosted_in", h.objectLinkPuts[0].linkType)
}

// TestSemanticTranslator_ReverseLinksSkipped verifies that type→instance reverse
// links (__object from a type, __type from an object to a type) are not dispatched.
func TestSemanticTranslator_ReverseLinksSkipped(t *testing.T) {
	h := &mockHandler{}
	tr := NewSemanticTranslator(h)

	err := tr.ProcessEvent(makeEvent(
		linkPut("hub/root", "hub/types", "hub/types", "__types", "{}"),
		linkPut("hub/root", "hub/objects", "hub/objects", "__objects", "{}"),
		linkPut("hub/types", "hub/server", "hub/server", "__type", "{}"),
		linkPut("hub/objects", "hub/srv-0", "hub/srv-0", "__object", "{}"),
		// Object → its type (mapping, not a relationship link).
		linkPut("hub/srv-0", "hub/server", "type", "__type", "{}"),
		// Type → instance (reverse, must be ignored).
		linkPut("hub/server", "hub/srv-0", "hub/srv-0", "__object", "{}"),
		vertexPut("hub/srv-0", "{}"),
	))
	require.NoError(t, err)

	assert.Empty(t, h.objectLinkPuts, "type→instance reverse link must not be dispatched")
	assert.Empty(t, h.typeLinkPuts)
	require.Len(t, h.objectPuts, 1) // only srv-0 is an object
}

// TestSemanticTranslator_TypeDelete verifies OnTypeDelete is called when a
// classification __type link from the types-hub is deleted.
func TestSemanticTranslator_TypeDelete(t *testing.T) {
	h := &mockHandler{}
	tr := NewSemanticTranslator(h)

	// First event: classify hub/server as a type.
	require.NoError(t, tr.ProcessEvent(makeEvent(
		linkPut("hub/root", "hub/types", "hub/types", "__types", "{}"),
		linkPut("hub/types", "hub/server", "hub/server", "__type", "{}"),
		vertexPut("hub/server", "{}"),
	)))

	// Second event: remove the classification link → type is deleted.
	require.NoError(t, tr.ProcessEvent(makeEvent(
		linkDeleteWithType("hub/types", "hub/server", "hub/server", "__type"),
		vertexDelete("hub/server"),
	)))

	assert.Equal(t, []string{"hub/server"}, h.typeDeletes)
}

// TestSemanticTranslator_ObjectDelete verifies OnObjectDelete is called for a
// deleted object vertex.
func TestSemanticTranslator_ObjectDelete(t *testing.T) {
	h := &mockHandler{}
	tr := NewSemanticTranslator(h)

	require.NoError(t, tr.ProcessEvent(makeEvent(
		linkPut("hub/root", "hub/types", "hub/types", "__types", "{}"),
		linkPut("hub/root", "hub/objects", "hub/objects", "__objects", "{}"),
		linkPut("hub/types", "hub/server", "hub/server", "__type", "{}"),
		linkPut("hub/objects", "hub/srv-0", "hub/srv-0", "__object", "{}"),
		linkPut("hub/srv-0", "hub/server", "type", "__type", "{}"),
		vertexPut("hub/srv-0", "{}"),
	)))

	require.NoError(t, tr.ProcessEvent(makeEvent(
		linkDeleteWithType("hub/objects", "hub/srv-0", "hub/srv-0", "__object"),
		vertexDelete("hub/srv-0"),
	)))

	assert.Equal(t, []string{"hub/srv-0"}, h.objectDeletes)
}

// TestSemanticTranslator_CrossEventHubPersistence verifies that typesHubs and
// objectsHubs persist across events so that classification works when the
// structural links (root→hub/types) arrived in an earlier event.
func TestSemanticTranslator_CrossEventHubPersistence(t *testing.T) {
	h := &mockHandler{}
	tr := NewSemanticTranslator(h)

	// Event 1: only the structural links (hub identification).
	require.NoError(t, tr.ProcessEvent(makeEvent(
		linkPut("hub/root", "hub/types", "hub/types", "__types", "{}"),
		linkPut("hub/root", "hub/objects", "hub/objects", "__objects", "{}"),
	)))

	// Event 2: type and object creation — classification must work without seeing
	// the structural links again.
	require.NoError(t, tr.ProcessEvent(makeEvent(
		linkPut("hub/types", "hub/device", "hub/device", "__type", "{}"),
		linkPut("hub/objects", "hub/dev-1", "hub/dev-1", "__object", "{}"),
		linkPut("hub/dev-1", "hub/device", "type", "__type", "{}"),
		vertexPut("hub/device", "{}"),
		vertexPut("hub/dev-1", `{"ip":"10.0.0.1"}`),
	)))

	require.Len(t, h.typePuts, 1)
	assert.Equal(t, "hub/device", h.typePuts[0].id)
	require.Len(t, h.objectPuts, 1)
	assert.Equal(t, "hub/dev-1", h.objectPuts[0].id)
	assert.Equal(t, "hub/device", h.objectPuts[0].typeID)
}

// TestSemanticTranslator_UnknownVerticesSkipped verifies that vertices that
// cannot be classified (root, hubs, unknown) are not dispatched.
func TestSemanticTranslator_UnknownVerticesSkipped(t *testing.T) {
	h := &mockHandler{}
	tr := NewSemanticTranslator(h)

	err := tr.ProcessEvent(makeEvent(
		linkPut("hub/root", "hub/types", "hub/types", "__types", "{}"),
		linkPut("hub/root", "hub/objects", "hub/objects", "__objects", "{}"),
		vertexPut("hub/root", "{}"),
		vertexPut("hub/types", "{}"),
		vertexPut("hub/objects", "{}"),
		vertexPut("hub/unknown", "{}"),
	))
	require.NoError(t, err)

	assert.Empty(t, h.typePuts)
	assert.Empty(t, h.objectPuts)
}

// TestSemanticTranslator_ObjectLinkDelete verifies OnObjectLinkDelete is called
// when a link between two objects is deleted.
func TestSemanticTranslator_ObjectLinkDelete(t *testing.T) {
	h := &mockHandler{}
	tr := NewSemanticTranslator(h)

	// Classify two objects in event 1.
	require.NoError(t, tr.ProcessEvent(makeEvent(
		linkPut("hub/root", "hub/types", "hub/types", "__types", "{}"),
		linkPut("hub/root", "hub/objects", "hub/objects", "__objects", "{}"),
		linkPut("hub/types", "hub/server", "hub/server", "__type", "{}"),
		linkPut("hub/objects", "hub/srv-0", "hub/srv-0", "__object", "{}"),
		linkPut("hub/objects", "hub/rack-A", "hub/rack-A", "__object", "{}"),
		vertexPut("hub/srv-0", "{}"),
		vertexPut("hub/rack-A", "{}"),
	)))

	// Delete the object-to-object link in event 2.
	require.NoError(t, tr.ProcessEvent(makeEvent(
		linkDelete("hub/srv-0", "rack-link-0"),
	)))

	require.Len(t, h.objectLinkDels, 1)
	assert.Equal(t, "hub/srv-0", h.objectLinkDels[0].from)
	assert.Equal(t, "rack-link-0", h.objectLinkDels[0].name)
}

// TestSemanticTranslator_TypeLinkDelete verifies OnTypeLinkDelete is called
// when a link between two types is deleted.
func TestSemanticTranslator_TypeLinkDelete(t *testing.T) {
	h := &mockHandler{}
	tr := NewSemanticTranslator(h)

	require.NoError(t, tr.ProcessEvent(makeEvent(
		linkPut("hub/root", "hub/types", "hub/types", "__types", "{}"),
		linkPut("hub/types", "hub/server", "hub/server", "__type", "{}"),
		linkPut("hub/types", "hub/rack", "hub/rack", "__type", "{}"),
		vertexPut("hub/server", "{}"),
		vertexPut("hub/rack", "{}"),
	)))

	require.NoError(t, tr.ProcessEvent(makeEvent(
		linkDelete("hub/server", "hub/rack"),
	)))

	require.Len(t, h.typeLinkDels, 1)
	assert.Equal(t, "hub/server", h.typeLinkDels[0].from)
	assert.Equal(t, "hub/rack", h.typeLinkDels[0].name)
}

// TestSemanticTranslator_ObjectTypeEmptyWhenUnknown verifies that OnObjectPut
// is still called with an empty typeID when the object's __type link has not
// been seen yet.
func TestSemanticTranslator_ObjectTypeEmptyWhenUnknown(t *testing.T) {
	h := &mockHandler{}
	tr := NewSemanticTranslator(h)

	err := tr.ProcessEvent(makeEvent(
		linkPut("hub/root", "hub/types", "hub/types", "__types", "{}"),
		linkPut("hub/root", "hub/objects", "hub/objects", "__objects", "{}"),
		linkPut("hub/objects", "hub/srv-0", "hub/srv-0", "__object", "{}"),
		// No __type link for hub/srv-0 → typeID unknown.
		vertexPut("hub/srv-0", "{}"),
	))
	require.NoError(t, err)

	require.Len(t, h.objectPuts, 1)
	assert.Equal(t, "hub/srv-0", h.objectPuts[0].id)
	assert.Empty(t, h.objectPuts[0].typeID)
}

// TestSemanticTranslator_FullCMDBScenario is a full-scenario test that mirrors
// the actual export event observed in integration testing. It verifies that a
// single event containing bootstrap links, CRUD operations, and a delete all
// produce the correct semantic calls.
func TestSemanticTranslator_FullCMDBScenario(t *testing.T) {
	h := &mockHandler{}
	tr := NewSemanticTranslator(h)

	// This mirrors the structure of the actual export event seen in docker-compose
	// testing: one WAL transaction containing bootstrap + user CRUD.
	err := tr.ProcessEvent(makeEvent(
		// Bootstrap structural links.
		linkPut("hub/root", "hub/types", "hub/types", "__types", "{}"),
		linkPut("hub/root", "hub/objects", "hub/objects", "__objects", "{}"),
		// Type classification links (hub/types → actual types via __type).
		linkPut("hub/types", "hub/server", "hub/server", "__type", "{}"),
		linkPut("hub/types", "hub/rack", "hub/rack", "__type", "{}"),
		// Type-to-type relationship.
		linkPut("hub/server", "hub/rack", "hub/rack", "__type", `{"type":"hosted_in"}`),
		// Object classification links (hub/objects → actual objects via __object).
		linkPut("hub/objects", "hub/srv-0", "hub/srv-0", "__object", "{}"),
		linkPut("hub/objects", "hub/srv-1", "hub/srv-1", "__object", "{}"),
		linkPut("hub/objects", "hub/rack-A", "hub/rack-A", "__object", "{}"),
		// hub/srv-2 is not added in this event (only deleted) — it was never
		// classified, so vertex_delete for it will be a no-op.
		linkDeleteWithType("hub/objects", "hub/srv-2", "hub/srv-2", "__object"),
		// Object → type associations (via __type).
		linkPut("hub/srv-0", "hub/server", "type", "__type", "{}"),
		linkPut("hub/srv-1", "hub/server", "type", "__type", "{}"),
		linkPut("hub/rack-A", "hub/rack", "type", "__type", "{}"),
		// Type → instance reverse links (must be ignored).
		linkPut("hub/server", "hub/srv-0", "hub/srv-0", "__object", "{}"),
		linkPut("hub/server", "hub/srv-1", "hub/srv-1", "__object", "{}"),
		linkPut("hub/rack", "hub/rack-A", "hub/rack-A", "__object", "{}"),
		// Object-to-object links.
		linkPut("hub/srv-0", "hub/rack-A", "rack-link-0", "hosted_in", "{}"),
		linkPut("hub/srv-1", "hub/rack-A", "rack-link-1", "hosted_in", "{}"),
		// Vertex bodies.
		vertexPut("hub/server", `{"desc":"server"}`),
		vertexPut("hub/rack", `{"desc":"rack"}`),
		vertexPut("hub/srv-0", `{"name":"srv-0"}`),
		vertexPut("hub/srv-1", `{"name":"srv-1"}`),
		vertexPut("hub/rack-A", `{"location":"DC1"}`),
		vertexDelete("hub/srv-2"),
		// Hub / root vertices — must be skipped.
		vertexPut("hub/root", "{}"),
		vertexPut("hub/types", "{}"),
		vertexPut("hub/objects", "{}"),
	))
	require.NoError(t, err)

	// Types.
	typeIDs := make(map[string]string)
	for _, tp := range h.typePuts {
		typeIDs[tp.id] = tp.body
	}
	assert.Contains(t, typeIDs, "hub/server")
	assert.Contains(t, typeIDs, "hub/rack")
	assert.NotContains(t, typeIDs, "hub/types", "hub/types must not be classified as a type")
	assert.NotContains(t, typeIDs, "hub/root")

	// Objects.
	objMap := make(map[string]objectPutCall)
	for _, op := range h.objectPuts {
		objMap[op.id] = op
	}
	assert.Contains(t, objMap, "hub/srv-0")
	assert.Equal(t, "hub/server", objMap["hub/srv-0"].typeID)
	assert.Contains(t, objMap["hub/srv-0"].body, "srv-0")
	assert.Contains(t, objMap, "hub/srv-1")
	assert.Equal(t, "hub/server", objMap["hub/srv-1"].typeID)
	assert.Contains(t, objMap, "hub/rack-A")
	assert.Equal(t, "hub/rack", objMap["hub/rack-A"].typeID)
	assert.NotContains(t, objMap, "hub/objects", "hub/objects must not be classified as an object")
	assert.NotContains(t, objMap, "hub/root")

	// hub/srv-2 was never classified (no link_put for it in this event),
	// so its vertex_delete is a no-op — objectDeletes should NOT contain it.
	assert.NotContains(t, h.objectDeletes, "hub/srv-2")

	// Type link.
	require.Len(t, h.typeLinkPuts, 1)
	assert.Equal(t, "hub/server", h.typeLinkPuts[0].from)
	assert.Equal(t, "hub/rack", h.typeLinkPuts[0].to)

	// Object links.
	objLinkMap := make(map[string]objectLinkPutCall)
	for _, lp := range h.objectLinkPuts {
		objLinkMap[lp.name] = lp
	}
	assert.Contains(t, objLinkMap, "rack-link-0")
	assert.Contains(t, objLinkMap, "rack-link-1")
}

// TestSemanticTranslator_MultipleObjects verifies that multiple objects of
// the same type are all classified and dispatched correctly.
func TestSemanticTranslator_MultipleObjects(t *testing.T) {
	const numObjects = 10

	h := &mockHandler{}
	tr := NewSemanticTranslator(h)

	ops := []ExportOp{
		linkPut("hub/root", "hub/types", "hub/types", "__types", "{}"),
		linkPut("hub/root", "hub/objects", "hub/objects", "__objects", "{}"),
		linkPut("hub/types", "hub/node", "hub/node", "__type", "{}"),
		vertexPut("hub/node", "{}"),
	}
	for i := 0; i < numObjects; i++ {
		id := "hub/node-" + string(rune('0'+i))
		ops = append(ops,
			linkPut("hub/objects", id, id, "__object", "{}"),
			linkPut(id, "hub/node", "type", "__type", "{}"),
			vertexPut(id, "{}"),
		)
	}

	require.NoError(t, tr.ProcessEvent(makeEvent(ops...)))

	assert.Len(t, h.typePuts, 1)
	assert.Len(t, h.objectPuts, numObjects)
	for _, op := range h.objectPuts {
		assert.Equal(t, "hub/node", op.typeID)
	}
}

// TestSemanticTranslator_HandlerErrorPropagated verifies that errors returned
// by the handler are propagated back to the caller.
func TestSemanticTranslator_HandlerErrorPropagated(t *testing.T) {
	expectedErr := assert.AnError
	h := &mockHandler{returnErr: expectedErr}
	tr := NewSemanticTranslator(h)

	err := tr.ProcessEvent(makeEvent(
		linkPut("hub/root", "hub/types", "hub/types", "__types", "{}"),
		linkPut("hub/types", "hub/server", "hub/server", "__type", "{}"),
		vertexPut("hub/server", "{}"),
	))
	assert.ErrorIs(t, err, expectedErr)
}

// TestSemanticTranslator_EmptyEvent verifies that an empty event causes no
// handler calls and returns nil.
func TestSemanticTranslator_EmptyEvent(t *testing.T) {
	h := &mockHandler{}
	tr := NewSemanticTranslator(h)

	require.NoError(t, tr.ProcessEvent(makeEvent()))

	assert.Empty(t, h.typePuts)
	assert.Empty(t, h.objectPuts)
	assert.Empty(t, h.objectLinkPuts)
	assert.Empty(t, h.typeLinkPuts)
}

// TestSemanticTranslator_BodyOnlyObjectLinkUpdate verifies that a link_put with
// empty To and LinkType (body-only update, as produced by LLAPILinkUpdate) is
// correctly dispatched using the target cached from the original link creation.
func TestSemanticTranslator_BodyOnlyObjectLinkUpdate(t *testing.T) {
	h := &mockHandler{}
	tr := NewSemanticTranslator(h)

	// Event 1: classify two objects and create a link between them.
	require.NoError(t, tr.ProcessEvent(makeEvent(
		linkPut("hub/root", "hub/types", "hub/types", "__types", "{}"),
		linkPut("hub/root", "hub/objects", "hub/objects", "__objects", "{}"),
		linkPut("hub/types", "hub/server", "hub/server", "__type", "{}"),
		linkPut("hub/objects", "hub/srv-0", "hub/srv-0", "__object", "{}"),
		linkPut("hub/objects", "hub/rack-A", "hub/rack-A", "__object", "{}"),
		linkPut("hub/srv-0", "hub/server", "type", "__type", "{}"),
		linkPut("hub/rack-A", "hub/server", "type", "__type", "{}"),
		linkPut("hub/srv-0", "hub/rack-A", "rack-link-0", "hosted_in", `{"weight":1}`),
	)))

	h.objectLinkPuts = nil // reset

	// Event 2: body-only update — To and LinkType are empty (target didn't change).
	bodyOnly := ExportOp{
		Op:   "link_put",
		From: "hub/srv-0",
		To:   "",
		Name: "rack-link-0",
		// LinkType intentionally empty — simulates WAL body-only update.
		Body: json.RawMessage(`{"weight":2}`),
	}
	require.NoError(t, tr.ProcessEvent(makeEvent(bodyOnly)))

	require.Len(t, h.objectLinkPuts, 1, "body-only update must not be dropped")
	assert.Equal(t, "hub/srv-0", h.objectLinkPuts[0].from)
	assert.Equal(t, "hub/rack-A", h.objectLinkPuts[0].to, "To must be restored from linkTargets")
	assert.Equal(t, "rack-link-0", h.objectLinkPuts[0].name)
	assert.Equal(t, "hosted_in", h.objectLinkPuts[0].linkType, "LinkType must be restored from linkTargets")
	assert.Contains(t, h.objectLinkPuts[0].body, `"weight":2`)
}

// TestSemanticTranslator_BodyOnlyTypeLinkUpdate verifies that body-only link_put
// for a type-to-type link is correctly dispatched with the cached target.
func TestSemanticTranslator_BodyOnlyTypeLinkUpdate(t *testing.T) {
	h := &mockHandler{}
	tr := NewSemanticTranslator(h)

	// Event 1: classify two types and create a type link.
	require.NoError(t, tr.ProcessEvent(makeEvent(
		linkPut("hub/root", "hub/types", "hub/types", "__types", "{}"),
		linkPut("hub/types", "hub/server", "hub/server", "__type", "{}"),
		linkPut("hub/types", "hub/rack", "hub/rack", "__type", "{}"),
		linkPut("hub/server", "hub/rack", "hub/rack", "__type", `{"type":"hosted_in"}`),
	)))

	h.typeLinkPuts = nil // reset

	// Event 2: body-only type link update (To and LinkType empty).
	bodyOnly := ExportOp{
		Op:   "link_put",
		From: "hub/server",
		To:   "",
		Name: "hub/rack",
		Body: json.RawMessage(`{"type":"hosted_in","meta":"updated"}`),
	}
	require.NoError(t, tr.ProcessEvent(makeEvent(bodyOnly)))

	require.Len(t, h.typeLinkPuts, 1, "body-only type link update must not be dropped")
	assert.Equal(t, "hub/server", h.typeLinkPuts[0].from)
	assert.Equal(t, "hub/rack", h.typeLinkPuts[0].to, "To must be restored from linkTargets")
	assert.Equal(t, "hosted_in", h.typeLinkPuts[0].linkType, "linkType must be extracted from body")
}

// TestSemanticTranslator_BodyOnlyLinkAfterDelete verifies that after a link_delete
// the linkTargets entry is removed, so a subsequent body-only link_put for the
// same name is not incorrectly dispatched using the stale target.
func TestSemanticTranslator_BodyOnlyLinkAfterDelete(t *testing.T) {
	h := &mockHandler{}
	tr := NewSemanticTranslator(h)

	// Event 1: classify objects and create link.
	require.NoError(t, tr.ProcessEvent(makeEvent(
		linkPut("hub/root", "hub/types", "hub/types", "__types", "{}"),
		linkPut("hub/root", "hub/objects", "hub/objects", "__objects", "{}"),
		linkPut("hub/types", "hub/server", "hub/server", "__type", "{}"),
		linkPut("hub/objects", "hub/srv-0", "hub/srv-0", "__object", "{}"),
		linkPut("hub/objects", "hub/rack-A", "hub/rack-A", "__object", "{}"),
		linkPut("hub/srv-0", "hub/server", "type", "__type", "{}"),
		linkPut("hub/rack-A", "hub/server", "type", "__type", "{}"),
		linkPut("hub/srv-0", "hub/rack-A", "rack-link-0", "hosted_in", "{}"),
	)))

	// Event 2: delete the link — must purge linkTargets entry.
	require.NoError(t, tr.ProcessEvent(makeEvent(
		linkDelete("hub/srv-0", "rack-link-0"),
	)))

	h.objectLinkPuts = nil

	// Event 3: body-only link_put with same name after deletion.
	// There is no cached target, so it must be dropped (no From/To classification).
	bodyOnly := ExportOp{
		Op:   "link_put",
		From: "hub/srv-0",
		To:   "",
		Name: "rack-link-0",
		Body: json.RawMessage(`{"weight":99}`),
	}
	require.NoError(t, tr.ProcessEvent(makeEvent(bodyOnly)))

	assert.Empty(t, h.objectLinkPuts, "stale body-only update after delete must be dropped")
}

// TestSemanticTranslator_ObjectLinkTags verifies that tags are forwarded to
// OnObjectLinkPut.
func TestSemanticTranslator_ObjectLinkTags(t *testing.T) {
	h := &mockHandler{}
	tr := NewSemanticTranslator(h)

	require.NoError(t, tr.ProcessEvent(makeEvent(
		linkPut("hub/root", "hub/types", "hub/types", "__types", "{}"),
		linkPut("hub/root", "hub/objects", "hub/objects", "__objects", "{}"),
		linkPut("hub/types", "hub/server", "hub/server", "__type", "{}"),
		linkPut("hub/objects", "hub/srv-0", "hub/srv-0", "__object", "{}"),
		linkPut("hub/objects", "hub/srv-1", "hub/srv-1", "__object", "{}"),
	)))

	// Second event: link with tags.
	op := ExportOp{
		Op:       "link_put",
		From:     "hub/srv-0",
		To:       "hub/srv-1",
		Name:     "peer",
		LinkType: "peer_link",
		Tags:     []string{"primary", "active"},
		Body:     json.RawMessage("{}"),
	}
	require.NoError(t, tr.ProcessEvent(makeEvent(op)))

	require.Len(t, h.objectLinkPuts, 1)
	assert.Equal(t, []string{"primary", "active"}, h.objectLinkPuts[0].tags)
}
