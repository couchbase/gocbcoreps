package gocbcoreps

import "errors"

var (
	// ErrAuthenticatorMismatch is returned when there is a mismatch between authenticators.
	ErrAuthenticatorMismatch = errors.New("authenticator mismatch")

	// ErrAuthenticatorUnsupported is returned when a unsupported authenticator is specified.
	ErrAuthenticatorUnsupported = errors.New("authenticator unsupported")
)
