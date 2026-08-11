# ADR 003: Tokenize identity before durable analytical storage

Status: accepted

The gateway converts the normalized source user identifier to a keyed HMAC token. The event
contract accepts only the token after that boundary, and recursive checks reject credentials,
cookies and card fields. Quarantine stores a payload hash and reason rather than the raw secret.

This preserves stable analytical linkage while reducing exposure. Key rotation still requires a
versioned token strategy and an approved migration plan in production.
