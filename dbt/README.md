# dbt

The dbt project lives in [`transform/`](../transform/), as the course project
brief names it. Nothing is built from this folder.

- Models, tests and sources: `transform/models/`, `transform/tests/`
- How to build it: [acceptance criteria](../docs/acceptance-criteria.md)
- Lineage: [docs/lineage.md](../docs/lineage.md)
- Materialisation choice: [ADR 0005](../docs/adr/0005-materialisations.md)

In **L06** the dbt runner gets its Dockerfile in `transform/`. In **L10** the
PII-redaction model joins the project.
