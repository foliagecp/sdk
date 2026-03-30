package statefun

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/foliagecp/sdk/statefun/cache"
	lg "github.com/foliagecp/sdk/statefun/logger"
	"github.com/nats-io/nats.go"
)

const (
	ExportStreamNameTmpl = "export-%s-events"
	ExportSubjectTmpl    = "export.%s.events"

	// --- Export stream defaults ---

	// Retention cap on the main export events stream (per-domain).
	ExportStreamMaxMsgs  = 100_000
	ExportStreamMaxBytes = 512 * 1024 * 1024 // 512 MB
	// How long to keep export events before they are discarded.
	ExportStreamMaxAge = 72 * time.Hour

	// ExportEnabled is the compile-time default; overridden by SetExportEnabled.
	ExportEnabled = false

	// --- WAL value format ---

	// WAL values carry an 8-byte big-endian timestamp followed by a 1-byte flags field.
	walValueHeaderSize     = 9
	walFlagDeleted    byte = 0x02 // tombstone — entry was deleted
)

// WALOp represents a raw operation from the WAL.
type WALOp struct {
	OpType string // cache.OpTypePUT or cache.OpTypeDelete
	Key    string // Full KV key (with store prefix)
	Value  []byte // Serialized data (9-byte header + payload)
}

// ExportEvent is the semantic event published to the export stream.
type ExportEvent struct {
	TxID      string     `json:"tx_id"`
	Domain    string     `json:"domain"`
	Timestamp int64      `json:"timestamp"`
	Ops       []ExportOp `json:"ops"`
}

// ExportOp is a single semantic operation within an export event.
type ExportOp struct {
	Op       string          `json:"op"`
	ID       string          `json:"id,omitempty"`
	Body     json.RawMessage `json:"body,omitempty"`
	From     string          `json:"from,omitempty"`
	To       string          `json:"to,omitempty"`
	Name     string          `json:"name,omitempty"`
	LinkType string          `json:"link_type,omitempty"`
	Tags     []string        `json:"tags,omitempty"`
}

// parsedKey holds the result of parsing a KV key into its semantic components.
type parsedKey struct {
	entityType string // "vertex", "link", or "aux" (auxiliary/index keys to ignore)
	entityKey  string // unique key for deduplication: vertexID or "from\x00name"
	role       string // "body", "target", "tag", "type_index", "in_link", "body_index"
	vertexID   string // for vertex keys
	from       string // for link keys
	linkName   string // for link keys
	tagValue   string // for tag index keys
}

// vertexAccum accumulates data for a vertex during deduplication.
type vertexAccum struct {
	id      string
	body    json.RawMessage
	deleted bool
}

// linkAccum accumulates data for a link during deduplication.
type linkAccum struct {
	from     string
	to       string
	name     string
	linkType string
	body     json.RawMessage
	tags     []string
	deleted  bool
}

// ExportCommitter collects WAL operations per transaction,
// deduplicates them into semantic events, and publishes to the export stream.
type ExportCommitter struct {
	js         nats.JetStreamContext
	domain     string
	streamName string
	subject    string
}

// NewExportCommitter creates a new ExportCommitter.
func NewExportCommitter(js nats.JetStreamContext, domain string) *ExportCommitter {
	return &ExportCommitter{
		js:         js,
		domain:     domain,
		streamName: fmt.Sprintf(ExportStreamNameTmpl, domain),
		subject:    fmt.Sprintf(ExportSubjectTmpl, domain),
	}
}

// CreateExportStream creates the NATS JetStream stream for export events.
func (ec *ExportCommitter) CreateExportStream(maxMsgs, maxBytes int64, maxAge time.Duration, replicasCount int) error {
	sc := &nats.StreamConfig{
		Name:      ec.streamName,
		Subjects:  []string{ec.subject},
		Retention: nats.LimitsPolicy,
		Replicas:  replicasCount,
		MaxMsgs:   maxMsgs,
		MaxBytes:  maxBytes,
		MaxAge:    maxAge,
	}

	if _, err := ec.js.StreamInfo(ec.streamName); err == nil {
		return nil // already exists
	}

	_, err := ec.js.AddStream(sc)
	return err
}

// ProcessTransaction takes raw WAL operations for a transaction,
// deduplicates them into semantic events, and publishes to the export stream.
func (ec *ExportCommitter) ProcessTransaction(txID string, ops []WALOp, storePrefix string) error {
	if len(ops) == 0 {
		return nil
	}

	semanticOps := deduplicateOps(ops, storePrefix)
	if len(semanticOps) == 0 {
		return nil
	}

	event := ExportEvent{
		TxID:      txID,
		Domain:    ec.domain,
		Timestamp: time.Now().UnixNano(),
		Ops:       semanticOps,
	}

	data, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal export event: %w", err)
	}

	if _, err := ec.js.Publish(ec.subject, data); err != nil {
		lg.Logf(lg.ErrorLevel, "ExportCommitter: failed to publish event for tx=%s: %s", txID, err)
		return err
	}

	lg.Logf(lg.TraceLevel, "ExportCommitter: published %d semantic ops for tx=%s", len(semanticOps), txID)
	return nil
}

// StreamName returns the name of the export stream.
func (ec *ExportCommitter) StreamName() string {
	return ec.streamName
}

// Subject returns the subject of the export stream.
func (ec *ExportCommitter) Subject() string {
	return ec.subject
}

// deduplicateOps converts raw WAL operations into semantic export operations.
func deduplicateOps(ops []WALOp, storePrefix string) []ExportOp {
	vertices := make(map[string]*vertexAccum)  // keyed by vertexID
	links := make(map[string]*linkAccum)        // keyed by "from\x00name"
	vertexOrder := []string{}                    // preserve insertion order
	linkOrder := []string{}

	for _, op := range ops {
		pk := parseKey(op.Key, storePrefix)
		if pk == nil || pk.entityType == "aux" {
			continue
		}

		switch pk.entityType {
		case "vertex":
			acc, exists := vertices[pk.entityKey]
			if !exists {
				acc = &vertexAccum{id: pk.vertexID}
				vertices[pk.entityKey] = acc
				vertexOrder = append(vertexOrder, pk.entityKey)
			}

			switch pk.role {
			case "body":
				if op.OpType == cache.OpTypeDelete {
					acc.deleted = true
				} else {
					acc.deleted = false
					acc.body = extractBody(op.Value)
				}
			}

		case "link":
			acc, exists := links[pk.entityKey]
			if !exists {
				acc = &linkAccum{from: pk.from, name: pk.linkName}
				links[pk.entityKey] = acc
				linkOrder = append(linkOrder, pk.entityKey)
			}

			switch pk.role {
			case "target":
				if op.OpType == cache.OpTypeDelete {
					acc.deleted = true
				} else {
					acc.deleted = false
					// Value format: "<linkType>.<toVertexId>"
					if target := extractTargetValue(op.Value); target != "" {
						parts := strings.SplitN(target, ".", 2)
						if len(parts) == 2 {
							acc.linkType = parts[0]
							acc.to = parts[1]
						}
					}
				}
			case "body":
				if op.OpType != cache.OpTypeDelete {
					acc.body = extractBody(op.Value)
				}
			case "tag":
				if op.OpType != cache.OpTypeDelete && pk.tagValue != "" {
					acc.tags = appendUnique(acc.tags, pk.tagValue)
				}
			}
		}
	}

	result := make([]ExportOp, 0, len(vertices)+len(links))

	for _, key := range vertexOrder {
		acc := vertices[key]
		if acc.deleted {
			result = append(result, ExportOp{Op: "vertex_delete", ID: acc.id})
		} else {
			eop := ExportOp{Op: "vertex_put", ID: acc.id}
			if len(acc.body) > 0 {
				eop.Body = acc.body
			}
			result = append(result, eop)
		}
	}

	for _, key := range linkOrder {
		acc := links[key]
		if acc.deleted {
			result = append(result, ExportOp{
				Op:   "link_delete",
				From: acc.from,
				Name: acc.name,
			})
		} else {
			eop := ExportOp{
				Op:       "link_put",
				From:     acc.from,
				To:       acc.to,
				Name:     acc.name,
				LinkType: acc.linkType,
			}
			if len(acc.body) > 0 {
				eop.Body = acc.body
			}
			if len(acc.tags) > 0 {
				eop.Tags = acc.tags
			}
			result = append(result, eop)
		}
	}

	return result
}

// parseKey parses a KV key into its semantic components.
// Key formats (after stripping store prefix):
//   - <vertexId>                                           → vertex body
//   - <vertexId>.bodyindex.<key>.<type>                    → vertex body index (aux)
//   - <fromId>.out.to.<linkName>                           → link target
//   - <fromId>.out.body.<linkName>                         → link body
//   - <fromId>.ltype.<linkType>.<toId>                     → link type index (aux)
//   - <fromId>.out.index.<linkName>.<indexName>.<value>    → link index (tag if indexName=="tag")
//   - <fromId>.out.bodyindex.<linkName>.<key>.<type>       → link body index (aux)
//   - <toId>.in.<fromId>.<linkName>                        → in-link (aux)
//
// Dots are NOT allowed in vertex/link IDs (regex: [a-zA-Z0-9/_$#@%+=-]+),
// so "." is a safe delimiter.
func parseKey(key string, storePrefix string) *parsedKey {
	// Strip store prefix
	raw := key
	if storePrefix != "" {
		prefix := storePrefix + "."
		if strings.HasPrefix(key, prefix) {
			raw = key[len(prefix):]
		}
	}

	if raw == "" || raw == cache.ConsistencyKey {
		return nil
	}

	// Try to match known patterns by looking for structural segments.
	// Order matters: check more specific patterns first.

	// Link body index: <fromId>.out.bodyindex.<linkName>.<key>.<type>
	if idx := strings.Index(raw, ".out.bodyindex."); idx >= 0 {
		return &parsedKey{entityType: "aux"}
	}

	// Link target: <fromId>.out.to.<linkName>
	if idx := strings.Index(raw, ".out.to."); idx >= 0 {
		from := raw[:idx]
		linkName := raw[idx+len(".out.to."):]
		if from != "" && linkName != "" {
			return &parsedKey{
				entityType: "link",
				entityKey:  from + "\x00" + linkName,
				role:       "target",
				from:       from,
				linkName:   linkName,
			}
		}
	}

	// Link body: <fromId>.out.body.<linkName>
	if idx := strings.Index(raw, ".out.body."); idx >= 0 {
		from := raw[:idx]
		linkName := raw[idx+len(".out.body."):]
		if from != "" && linkName != "" {
			return &parsedKey{
				entityType: "link",
				entityKey:  from + "\x00" + linkName,
				role:       "body",
				from:       from,
				linkName:   linkName,
			}
		}
	}

	// Link index: <fromId>.out.index.<linkName>.<indexName>.<value>
	if idx := strings.Index(raw, ".out.index."); idx >= 0 {
		from := raw[:idx]
		rest := raw[idx+len(".out.index."):]
		// rest = <linkName>.<indexName>.<value>
		parts := strings.SplitN(rest, ".", 3)
		if len(parts) >= 2 && from != "" {
			linkName := parts[0]
			indexName := parts[1]
			if indexName == "tag" && len(parts) == 3 {
				return &parsedKey{
					entityType: "link",
					entityKey:  from + "\x00" + linkName,
					role:       "tag",
					from:       from,
					linkName:   linkName,
					tagValue:   parts[2],
				}
			}
			// Other index types are auxiliary
			return &parsedKey{entityType: "aux"}
		}
	}

	// Link type index: <fromId>.ltype.<linkType>.<toId>
	if idx := strings.Index(raw, ".ltype."); idx >= 0 {
		return &parsedKey{entityType: "aux"}
	}

	// In-link: <toId>.in.<fromId>.<linkName>
	if idx := strings.Index(raw, ".in."); idx >= 0 {
		return &parsedKey{entityType: "aux"}
	}

	// Vertex body index: <vertexId>.bodyindex.<key>.<type>
	if idx := strings.Index(raw, ".bodyindex."); idx >= 0 {
		return &parsedKey{entityType: "aux"}
	}

	// If no known pattern matched, this is a plain vertex body key: <vertexId>
	return &parsedKey{
		entityType: "vertex",
		entityKey:  raw,
		role:       "body",
		vertexID:   raw,
	}
}

// extractBody extracts the JSON body from a WAL value (strips 9-byte header).
func extractBody(value []byte) json.RawMessage {
	if len(value) <= walValueHeaderSize {
		return nil
	}
	flag := value[8]
	if flag == walFlagDeleted {
		return nil
	}
	data := value[walValueHeaderSize:]
	if len(data) == 0 {
		return nil
	}
	// Return a copy to avoid referencing the original slice
	result := make(json.RawMessage, len(data))
	copy(result, data)
	return result
}

// extractTargetValue extracts a string value from a WAL value (strips 9-byte header).
func extractTargetValue(value []byte) string {
	if len(value) <= walValueHeaderSize {
		return ""
	}
	flag := value[8]
	if flag == walFlagDeleted {
		return ""
	}
	return string(value[walValueHeaderSize:])
}

// ParseWALValueTimestamp extracts the timestamp from a WAL value header.
func ParseWALValueTimestamp(value []byte) int64 {
	if len(value) < 8 {
		return 0
	}
	return int64(binary.BigEndian.Uint64(value[:8]))
}

func appendUnique(slice []string, val string) []string {
	for _, s := range slice {
		if s == val {
			return slice
		}
	}
	return append(slice, val)
}
