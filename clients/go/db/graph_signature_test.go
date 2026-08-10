package db

// Downstream applications pin the sync-client methods in their OWN
// interfaces, so these signatures are FROZEN: adding a parameter — even a
// variadic one — changes the method type and silently breaks interface
// satisfaction in every consumer (learned the hard way when
// VertexReadDetailsV2 briefly grew a variadic and stopped satisfying
// downstream interfaces). New capabilities go into new methods
// (VertexReadDetailsV2Full). This assertion fails to compile the moment a
// frozen signature drifts.

import "github.com/foliagecp/easyjson"

var _ interface {
	VertexRead(id string, details ...bool) (easyjson.JSON, error)
	VertexReadDetailsV2(id string) (easyjson.JSON, error)
	VertexReadDetailsV2Full(id string, linkContent ...bool) (easyjson.JSON, error)
} = GraphSyncClient{}
