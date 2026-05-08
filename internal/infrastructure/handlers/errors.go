package handlers

import "errors"

var (
	// ErrInvalidEvent marks errors caused by bad input (schema/contract violations).
	// These should go to DLQ.
	ErrInvalidEvent = errors.New("invalid event")

	// ErrTransient marks errors caused by temporary infrastructure issues (db/network).
	// These are candidates for retry (not implemented yet).
	ErrTransient = errors.New("transient failure")
)

