# Contributing

Thanks for your interest in contributing! This project is a hybrid Rust + Python
codebase: a Rust core (HTTP/gRPC server) with Python workers.

## Setup

You need a stable Rust toolchain and [uv](https://docs.astral.sh/uv/):

```bash
uv sync   # installs Python deps and builds the Rust extension (maturin)
```

## Running tests

```bash
uv run pytest    # Python tests (python/tests)
cargo test       # Rust tests
```

CI runs both on every pull request; please make sure they pass locally before
opening a PR.

## Guidelines

- **Tests first**: new features and bug fixes should come with tests that fail
  without the change.
- **Code language**: all code, comments, docstrings, log and error messages
  must be in English. User-facing docs under `docs/` may be in Chinese.
- **Commit messages**: use [Conventional Commits](https://www.conventionalcommits.org/),
  e.g. `feat(config): ...`, `fix(server): ...`, `chore(release): ...`.
- **Scope**: keep PRs focused — one concern per PR. Don't refactor adjacent
  code that isn't part of your change.

## Pull requests

1. Fork the repo and create a branch from `main`.
2. Make your change with tests.
3. Open a PR describing the motivation and how you verified the change.
4. A maintainer will review; CI must be green before merge. PRs are
   squash-merged, so don't worry about a messy commit history on your branch.
