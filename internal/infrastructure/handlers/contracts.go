package handlers

import (
	"encoding/json"
	"fmt"
)

type payloadValidator func(raw json.RawMessage) error

var payloadValidators = map[string]payloadValidator{
	"user.created":            validateUserCreatedPayload,
	"transaction.authorized":  validateTransactionAuthorizedPayload,
	// Keep this list intentionally small and focused for the challenge.
}

func validatePayload(eventType string, raw json.RawMessage) error {
	v, ok := payloadValidators[eventType]
	if !ok {
		// Unknown types are accepted. The "contract" is only enforced for known demo types.
		return nil
	}
	return v(raw)
}

func validateUserCreatedPayload(raw json.RawMessage) error {
	obj, err := asObject(raw)
	if err != nil {
		return fmt.Errorf("payload must be an object: %w", err)
	}
	if !hasNonEmptyString(obj, "user_id") {
		return fmt.Errorf("payload.user_id is required")
	}
	if !hasNonEmptyString(obj, "email") {
		return fmt.Errorf("payload.email is required")
	}
	return nil
}

func validateTransactionAuthorizedPayload(raw json.RawMessage) error {
	obj, err := asObject(raw)
	if err != nil {
		return fmt.Errorf("payload must be an object: %w", err)
	}
	if !hasNonEmptyString(obj, "transaction_id") {
		return fmt.Errorf("payload.transaction_id is required")
	}

	amountRaw, ok := obj["amount"]
	if !ok {
		return fmt.Errorf("payload.amount is required")
	}
	amountObj, err := asObject(amountRaw)
	if err != nil {
		return fmt.Errorf("payload.amount must be an object: %w", err)
	}
	if !hasNumber(amountObj, "value") {
		return fmt.Errorf("payload.amount.value is required")
	}
	if !hasNonEmptyString(amountObj, "currency") {
		return fmt.Errorf("payload.amount.currency is required")
	}
	return nil
}

func asObject(raw json.RawMessage) (map[string]json.RawMessage, error) {
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(raw, &obj); err != nil {
		return nil, err
	}
	if obj == nil {
		return nil, fmt.Errorf("null")
	}
	return obj, nil
}

func hasNonEmptyString(obj map[string]json.RawMessage, key string) bool {
	raw, ok := obj[key]
	if !ok {
		return false
	}
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return false
	}
	return s != ""
}

func hasNumber(obj map[string]json.RawMessage, key string) bool {
	raw, ok := obj[key]
	if !ok {
		return false
	}
	var n float64
	return json.Unmarshal(raw, &n) == nil
}

