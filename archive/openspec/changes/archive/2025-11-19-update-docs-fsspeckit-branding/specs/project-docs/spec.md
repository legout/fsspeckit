# Capability: Project Documentation

## ADDED Requirements

### Requirement: Canonical Package Naming in Public Docs

The documentation SHALL consistently refer to the library as `fsspeckit` and
MUST NOT reference the historical `fsspec-utils` name in user-facing
installation and contribution guides.

#### Scenario: Installation docs use fsspeckit name

- **WHEN** a user reads `docs/installation.md` or the README to install the
  library
- **THEN** all commands use `fsspeckit` (for example `pip install fsspeckit`)
- **AND** no examples mention `fsspec-utils`.

#### Scenario: Contributing docs use fsspeckit name

- **WHEN** a contributor reads `docs/contributing.md`
- **THEN** the project name and issue template instructions refer to
  `fsspeckit`
- **AND** version references use the `fsspeckit` package name.

### Requirement: Installation Docs Match Published Extras

The installation documentation SHALL describe optional extras and supported
Python versions that match `pyproject.toml`.

#### Scenario: Extras match pyproject configuration

- **WHEN** a user reads the "Optional Cloud Provider Support" section in
  `docs/installation.md`
- **THEN** the documented extras include `aws`, `gcp`, and `azure`
- **AND** example commands match the extras in `pyproject.toml` (for example
  `pip install "fsspeckit[aws,gcp,azure]"`).

#### Scenario: Python version requirement is accurate

- **WHEN** a user checks the prerequisites for installing `fsspeckit`
- **THEN** the docs state Python 3.11+ to match `requires-python` in
  `pyproject.toml`.

### Requirement: API Docs Reflect Current Package Namespace

API and logging documentation that comes from docstrings SHALL describe the
`fsspeckit` package namespace and supported use-cases, not the historical
`fsspec-utils` name.

#### Scenario: Logging helper docstrings use fsspeckit

- **WHEN** a user opens the logging API docs
  (`docs/api/fsspeckit.utils.logging.md` or generated equivalent)
- **THEN** descriptions and parameter docs refer to configuring logging for the
  `fsspeckit` package
- **AND** no messages suggest installing or configuring `fsspec-utils`.

#### Scenario: Top-level package docstring uses fsspeckit

- **WHEN** a user inspects `fsspeckit.__doc__` or generated API docs for the
  root package
- **THEN** the description identifies the package as `fsspeckit`
- **AND** describes its purpose without mentioning `fsspec-utils`.

### Requirement: Example Docs Align with Package Branding

Example READMEs and top-of-file comments that are intended as documentation SHALL
use the `fsspeckit` name and install commands.

#### Scenario: Example READMEs use fsspeckit

- **WHEN** a user reads example READMEs under `examples/`
- **THEN** titles and prerequisite sections reference `fsspeckit`
- **AND** suggested install commands use `pip install fsspeckit` (or extras)
  instead of `fsspec-utils`.

#### Scenario: Example script headers use fsspeckit

- **WHEN** a user opens example Python scripts (for example
  `examples/caching/caching_example.py`)
- **THEN** module-level docstrings and comments mention `fsspeckit`
- **AND** narrative text stays consistent with the rest of the documentation.
