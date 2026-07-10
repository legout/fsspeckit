## ADDED Requirements

### Requirement: Examples Are Safe to Run (No Destructive Cleanup)

Example scripts that create temporary files/directories SHALL clean up only the
paths they created and MUST NOT delete broad parent directories.

#### Scenario: Safe cleanup behavior
- **WHEN** a schema/workflow example creates a temporary directory for its data
- **THEN** cleanup SHALL remove only that directory
- **AND** it SHALL not attempt to delete shared system paths such as `/tmp`.

#### Scenario: Workflow examples are local-first
- **WHEN** a workflow example demonstrates cloud-style datasets
- **THEN** it SHALL run offline by default using local simulation
- **AND** any real-cloud execution SHALL be gated behind explicit configuration.

