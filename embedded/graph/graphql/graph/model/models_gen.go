// Code generated by github.com/99designs/gqlgen, DO NOT EDIT.

package model

import (
	"github.com/foliagecp/sdk/embedded/graph/graphql/extra"
)

type Object struct {
	ID            string     `json:"id"`
	Type          string     `json:"type"`
	RequestFields extra.JSON `json:"requestFields,omitempty"`
}

type Query struct {
}
