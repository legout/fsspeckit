# project-docs Specification Delta

## ADDED Requirements

### Requirement: PyArrow Dataset Handler Class Documentation

The documentation SHALL include comprehensive coverage of PyArrow class-based dataset handlers (`PyarrowDatasetIO`, `PyarrowDatasetHandler`) with same level of detail as DuckDB handler documentation.

#### Scenario: API reference includes PyArrow classes
- **WHEN** a user reads `docs/api/fsspeckit.datasets.md`
- **THEN** they SHALL find complete documentation for `PyarrowDatasetIO` and `PyarrowDatasetHandler` classes
- **AND** all methods SHALL have parameter tables, return types, and examples
- **AND** documentation format SHALL match DuckDB class documentation structure.

#### Scenario: Dataset handlers guide shows both PyArrow approaches
- **WHEN** a user reads `docs/dataset-handlers.md`
- **THEN** they SHALL see both function-based and class-based PyArrow approaches documented
- **AND** class-based approach SHALL include strengths, features, and usage examples
- **AND** backend comparison table SHALL include PyArrow class-based approach.

#### Scenario: Getting started tutorial includes PyArrow examples
- **WHEN** a user follows `docs/tutorials/getting-started.md`
- **THEN** the "Your First Dataset Operation" section SHALL include PyArrow examples alongside DuckDB
- **AND** domain package structure examples SHALL include PyArrow handler imports
- **AND** common use cases SHALL show PyArrow handler usage.

#### Scenario: API guide capability overview includes PyArrow classes
- **WHEN** a user reads `docs/reference/api-guide.md`
- **THEN** the "Dataset Operations" capability SHALL list `PyarrowDatasetIO` and `PyarrowDatasetHandler`
- **AND** domain package organization SHALL describe PyArrow class-based functionality
- **AND** usage patterns SHALL include PyArrow handler examples.

#### Scenario: How-to guides demonstrate class-based PyArrow usage
- **WHEN** a user reads `docs/how-to/read-and-write-datasets.md` or `docs/how-to/merge-datasets.md`
- **THEN** they SHALL find examples using `PyarrowDatasetIO` and `PyarrowDatasetHandler`
- **AND** backend selection guidance SHALL include class-based PyArrow approach
- **AND** import statements SHALL show new PyArrow class imports.

### Requirement: PyArrow Approach Guidance Documentation

The documentation SHALL provide clear guidance on when to use function-based vs class-based PyArrow approaches with migration paths between them.

#### Scenario: Approach comparison and selection guidance
- **WHEN** a user evaluates PyArrow options in `docs/dataset-handlers.md`
- **THEN** they SHALL find clear comparison between function-based and class-based approaches
- **AND** use case recommendations SHALL be provided for each approach
- **AND** migration guidance SHALL be available for switching between approaches.

#### Scenario: Consistent cross-references between approaches
- **WHEN** a user reads PyArrow documentation
- **THEN** function-based and class-based approaches SHALL cross-reference each other
- **AND** examples SHALL show equivalent operations in both approaches
- **AND** import statements SHALL be clearly differentiated.