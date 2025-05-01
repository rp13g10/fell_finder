uv run ruff check --fix --extend-select='I' packages/fell_loader
uv run ruff check --fix --extend-select='I' packages/fell_viewer

uv run ruff format packages/fell_loader
uv run ruff format packages/fell_viewer

uv run vulture