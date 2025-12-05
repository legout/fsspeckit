## MODIFIED Requirements

### Requirement: Logged error messages avoid exposing raw credentials

AWS-related storage options SHALL avoid logging raw access keys, secrets, or tokens in error messages.

#### Scenario: Credential-related error is logged
- **WHEN** an error occurs in AWS storage configuration or usage
- **AND** the resulting exception contains credential-like values
- **THEN** the storage options layer SHALL scrub those values from the message before logging
- **AND** the log entry SHALL retain enough context to diagnose the error without exposing secrets.

