package statefun

import (
	"encoding/json"
	"sync"
)

// CMDB topology link types — mirror of embedded/graph/crud constants.
// Redefined here to avoid an import cycle (crud imports statefun).
//
// Actual semantics (derived from hl_crud_helpers.go and observed export events):
//   __types   : structural only — root → types-hub (hub/types). Identifies the hub.
//   __objects : structural only — root → objects-hub (hub/objects). Identifies the hub.
//   __type    : types-hub → type (classification), object → its type (mapping),
//               type → type (relationship link, dispatched as OnTypeLinkPut/Delete).
//   __object  : objects-hub → object (classification), type → instance (reverse, ignored).
const (
	cmdbObjectsTypelink = "__objects"
	cmdbTypesTypelink   = "__types"
	cmdbObjectTypelink  = "__object"
	cmdbToTypelink      = "__type"
)

// SemanticHandler receives high-level CMDB events translated from raw ExportEvents.
// All methods are called within a single ProcessEvent call (i.e. within one WAL
// transaction). The implementation should apply all changes atomically.
//
// Vertices that cannot be classified (system vertices such as "root", "types",
// "objects", plus any vertex whose classification link has not been seen yet)
// are silently skipped — they will appear in the next event once the topology
// links arrive.
type SemanticHandler interface {
	// --- Types ---
	OnTypePut(id string, body json.RawMessage) error
	OnTypeDelete(id string) error

	// --- Objects ---
	// typeID may be empty if the classification link has not been seen yet.
	OnObjectPut(id, typeID string, body json.RawMessage) error
	OnObjectDelete(id string) error

	// --- Links between objects ---
	// linkType is the semantic relationship type (e.g. "hosted_in", "has_nic").
	OnObjectLinkPut(from, to, name, linkType string, body json.RawMessage, tags []string) error
	OnObjectLinkDelete(from, name string) error

	// --- Links between types ---
	// linkType is the composition/interface type stored in the link body
	// (e.g. "hosted_in") for __type graph links, or "__sub" for subtype links.
	OnTypeLinkPut(from, to, name, linkType string, body json.RawMessage, tags []string) error
	OnTypeLinkDelete(from, name string) error
}

// entityKind classifies a vertex in the CMDB topology.
type entityKind int

const (
	kindUnknown entityKind = iota
	kindObject
	kindType
)

// linkMeta caches the target and graph link type of a known link so that
// body-only updates (where To and LinkType are empty in the ExportOp) can be
// resolved correctly.
type linkMeta struct {
	to       string
	linkType string
}

// SemanticTranslator translates raw ExportEvents into semantic SemanticHandler
// calls by maintaining an in-memory registry of vertex classifications.
//
// Classification is derived from topology link types using a multi-sub-pass
// algorithm within each event (see ProcessEvent for details). The registry
// (kinds, objectTypes, typesHubs, objectsHubs, linkTargets) persists across
// events so that incremental updates are handled correctly.
//
// Limitation: the registry is in-memory only. It is rebuilt from scratch on
// restart via the normal event stream. If the dumper process restarts, the
// first events will be processed with an empty registry and classified vertices
// will be skipped until their topology links are seen again. This limitation
// will be lifted when the KV→WAL startup-flush mechanism is implemented.
type SemanticTranslator struct {
	mu          sync.RWMutex
	kinds       map[string]entityKind // vertexID → kind
	objectTypes map[string]string     // objectID → typeID
	typesHubs   map[string]bool       // vertices identified as types-hubs (hub/types)
	objectsHubs map[string]bool       // vertices identified as objects-hubs (hub/objects)
	linkTargets map[string]linkMeta   // from+"\x00"+name → {to, linkType}
	handler     SemanticHandler
}

// NewSemanticTranslator creates a SemanticTranslator that calls handler for each
// classified semantic event.
func NewSemanticTranslator(handler SemanticHandler) *SemanticTranslator {
	return &SemanticTranslator{
		kinds:       make(map[string]entityKind),
		objectTypes: make(map[string]string),
		typesHubs:   make(map[string]bool),
		objectsHubs: make(map[string]bool),
		linkTargets: make(map[string]linkMeta),
		handler:     handler,
	}
}

// ProcessEvent translates a single ExportEvent into SemanticHandler calls.
// It is safe to call from multiple goroutines, but events should be processed
// in order — pass one event at a time from the Foliage function handler.
//
// Pass 1 (registry update) uses four ordered sub-passes to handle all link types
// correctly even when structural and classification links arrive in the same event:
//
//  1a. Identify hub vertices from __types / __objects structural links.
//  1b. Classify types: __type links FROM typesHubs.
//  1c. Classify objects: __object links FROM objectsHubs.
//  1d. Build objectTypes: __type links where From is a now-classified object.
//
// Pass 2 dispatches vertex ops (vertex_put / vertex_delete) and then non-structural
// link ops. __type links between two kindType vertices become OnTypeLinkPut/Delete;
// mixed-kind or hub-endpoint links are silently skipped.
func (t *SemanticTranslator) ProcessEvent(event ExportEvent) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	// --- Pass 0: update linkTargets cache ---
	// Persist (from, name) → {to, linkType} for all link_puts with a non-empty To
	// so that body-only updates (where To/LinkType are absent in the ExportOp) can
	// be resolved in dispatchLinkOp.
	for _, op := range event.Ops {
		key := op.From + "\x00" + op.Name
		if op.Op == "link_put" && op.To != "" {
			t.linkTargets[key] = linkMeta{to: op.To, linkType: op.LinkType}
		}
	}

	// --- Pass 1, sub-pass 1a: identify hub vertices from structural link_puts ---
	for _, op := range event.Ops {
		if op.Op == "link_put" {
			switch op.LinkType {
			case cmdbTypesTypelink:
				t.typesHubs[op.To] = true
			case cmdbObjectsTypelink:
				t.objectsHubs[op.To] = true
			}
		}
	}

	// --- Pass 1, sub-pass 1b: classify types (link_put only) ---
	// A __type link FROM a types-hub classifies the To vertex as a type.
	for _, op := range event.Ops {
		if op.Op == "link_put" && op.LinkType == cmdbToTypelink && t.typesHubs[op.From] {
			t.kinds[op.To] = kindType
		}
	}

	// --- Pass 1, sub-pass 1c: classify objects (link_put only) ---
	// A __object link FROM an objects-hub classifies the To vertex as an object.
	for _, op := range event.Ops {
		if op.Op == "link_put" && op.LinkType == cmdbObjectTypelink && t.objectsHubs[op.From] {
			t.kinds[op.To] = kindObject
		}
	}

	// --- Pass 1, sub-pass 1d: build objectTypes map (link_put only) ---
	// A __type link FROM a kindObject vertex records that object's type.
	// (After sub-passes 1b/1c, new classifications are already visible.)
	for _, op := range event.Ops {
		if op.Op == "link_put" && op.LinkType == cmdbToTypelink && t.kinds[op.From] == kindObject {
			t.objectTypes[op.From] = op.To
		}
	}

	// --- Pass 2a: dispatch vertex_put ---
	for _, op := range event.Ops {
		if op.Op == "vertex_put" {
			if err := t.dispatchVertexPut(op); err != nil {
				return err
			}
		}
	}

	// --- Pass 2b: dispatch vertex_delete ---
	// Dispatched BEFORE classification removals so the kind is still known.
	for _, op := range event.Ops {
		if op.Op == "vertex_delete" {
			if err := t.dispatchVertexDelete(op); err != nil {
				return err
			}
		}
	}

	// --- Pass 1, sub-pass 1e: remove kinds, objectTypes, and linkTargets (link_delete) ---
	for _, op := range event.Ops {
		if op.Op != "link_delete" {
			continue
		}
		delete(t.linkTargets, op.From+"\x00"+op.Name)
		switch op.LinkType {
		case cmdbTypesTypelink:
			delete(t.typesHubs, op.To)
		case cmdbObjectsTypelink:
			delete(t.objectsHubs, op.To)
		case cmdbToTypelink:
			if t.typesHubs[op.From] {
				delete(t.kinds, op.To)
			}
			if t.kinds[op.From] == kindObject {
				delete(t.objectTypes, op.From)
			}
		case cmdbObjectTypelink:
			if t.objectsHubs[op.From] {
				delete(t.kinds, op.To)
			}
		}
	}

	// --- Pass 2 (cont.): dispatch link ops ---
	//
	// Skip only the structural hub-identification links (__types / __objects).
	// __type and __object links are passed to dispatchLinkOp; mixed-kind or
	// hub-endpoint links are dropped there (hub vertices are kindUnknown).
	for _, op := range event.Ops {
		if op.Op != "link_put" && op.Op != "link_delete" {
			continue
		}
		if op.LinkType == cmdbTypesTypelink || op.LinkType == cmdbObjectsTypelink {
			continue
		}
		if err := t.dispatchLinkOp(op); err != nil {
			return err
		}
	}

	return nil
}

func (t *SemanticTranslator) dispatchVertexPut(op ExportOp) error {
	switch t.kinds[op.ID] {
	case kindObject:
		typeID := t.objectTypes[op.ID]
		return t.handler.OnObjectPut(op.ID, typeID, op.Body)
	case kindType:
		return t.handler.OnTypePut(op.ID, op.Body)
	default:
		// Unknown classification — skip silently.
		return nil
	}
}

func (t *SemanticTranslator) dispatchVertexDelete(op ExportOp) error {
	switch t.kinds[op.ID] {
	case kindObject:
		return t.handler.OnObjectDelete(op.ID)
	case kindType:
		return t.handler.OnTypeDelete(op.ID)
	default:
		return nil
	}
}

func (t *SemanticTranslator) dispatchLinkOp(op ExportOp) error {
	// Body-only link updates (LLAPILinkUpdate) do not re-write the target key, so
	// op.To and op.LinkType may be empty. Restore them from the linkTargets cache.
	if op.Op == "link_put" && op.To == "" {
		if meta, ok := t.linkTargets[op.From+"\x00"+op.Name]; ok {
			op.To = meta.to
			if op.LinkType == "" {
				op.LinkType = meta.linkType
			}
		}
	}

	fromKind := t.kinds[op.From]
	toKind := t.kinds[op.To]

	switch op.Op {
	case "link_put":
		switch {
		case fromKind == kindObject && toKind == kindObject:
			return t.handler.OnObjectLinkPut(op.From, op.To, op.Name, op.LinkType, op.Body, op.Tags)
		case fromKind == kindType && toKind == kindType:
			return t.handler.OnTypeLinkPut(op.From, op.To, op.Name, typeLinkType(op), op.Body, op.Tags)
		default:
			// Mixed or unknown endpoints — skip.
			return nil
		}

	case "link_delete":
		switch fromKind {
		case kindObject:
			return t.handler.OnObjectLinkDelete(op.From, op.Name)
		case kindType:
			return t.handler.OnTypeLinkDelete(op.From, op.Name)
		default:
			return nil
		}
	}

	return nil
}

// typeLinkType returns the semantic link type for a type-to-type link.
// For __type graph links the semantic type is stored in body["type"]
// (set by TypesLinkCreate via the object_type parameter). For other link
// types (e.g. __sub for subtype relationships) the graph link type is
// returned as-is.
func typeLinkType(op ExportOp) string {
	if op.LinkType == cmdbToTypelink && len(op.Body) > 0 {
		var b struct {
			Type string `json:"type"`
		}
		if json.Unmarshal(op.Body, &b) == nil && b.Type != "" {
			return b.Type
		}
	}
	return op.LinkType
}
