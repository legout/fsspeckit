## ADDED Requirements

### Requirement: Local-First Examples Respect DirFileSystem Base Paths

Examples that operate on temporary local data SHALL be written to respect the
default `DirFileSystem` base-path semantics of `fsspeckit.filesystem()`.

#### Scenario: Temp data is readable via rooted filesystem
- **WHEN** an example creates files under a temporary directory
- **THEN** it SHALL create its filesystem rooted at that directory (for example `filesystem(temp_dir, ...)`)
- **AND** it SHALL use relative patterns within that base for glob/exists/read operations.

#### Scenario: JSON examples use consistent read/write options
- **WHEN** an example writes dict-like JSON data and then reads it back
- **THEN** it SHALL use compatible `read_json` options (for example `as_dataframe=False`) or write data in a format matching the default read mode.

