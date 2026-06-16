# Contributing

Thanks for contributing to MoiraWeave MLOps.

## Workflow

1. Fork the repository.
2. Create a branch from `main` using one of these prefixes:
   - `feat/<short-topic>`
   - `fix/<short-topic>`
   - `workload/<short-topic>`
3. Implement the change with focused commits.
4. Run checks locally:

```bash
uv sync --all-packages --dev
make ci
```

5. Open a pull request using the PR template.

## Commit convention

Use Conventional Commits:

- `feat: ...`
- `fix: ...`
- `chore: ...`
- `docs: ...`
- `refactor: ...`
- `test: ...`

Breaking changes:

- `feat!: ...` or include `BREAKING CHANGE:` in the body.

## Pre-commit setup

```bash
uv run pre-commit install
uv run pre-commit run --all-files
```

## Pull request expectations

- Keep PRs small and reviewable.
- Include tests for behavioral changes.
- Update docs for user-facing changes.
- Link the issue being solved.

## Adding workload capabilities

1. Add or update workload schema support in the API gateway.
2. Add worker execution or adapter logic for the workload type.
3. Add sample manifests under the workload examples used by tests/docs.
4. Cover API, worker, and deployment behavior with focused tests.
5. Validate with `make ci`.
