# MkDocs Configuration Specification

## MODIFIED Requirements

### Requirement: Remove deprecated pypi_url configuration

The mkdocs.yml configuration file MUST not contain deprecated configuration parameters that cause build warnings. The `pypi_url` setting is no longer recognized by MkDocs and SHALL be removed to eliminate configuration warnings.
#### Scenario:
- When running `mkdocs build`, the configuration should not include deprecated `pypi_url` setting
- Given current mkdocs.yml contains `pypi_url: https://pypi.org/project/fsspeckit`
- When building documentation, MkDocs should not emit warning about unrecognised configuration name
- Then documentation builds cleanly without configuration-related warnings

### Requirement: Update mkdocstrings options structure

The mkdocstrings plugin configuration SHALL use the current non-deprecated options structure. Extra options MUST be moved from `options` to `options.extra` to eliminate deprecation warnings and ensure future compatibility with mkdocstrings.
#### Scenario:
- When running `mkdocs build`, mkdocstrings handler should use non-deprecated options structure
- Given current configuration has deprecated options directly under `options`
- When mkdocstrings processes Python documentation, it should use `options.extra` for extra parameters
- Then no deprecation warnings should be emitted about extra options placement
- And all existing documentation functionality should be preserved

## REMOVED Requirements

### Requirement: Support legacy pypi_url configuration
#### Scenario:
- MkDocs material theme no longer supports `pypi_url` configuration parameter
- The setting should be removed from mkdocs.yml without affecting site functionality
- Documentation site should continue to function normally without this setting