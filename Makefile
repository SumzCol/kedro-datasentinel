install-uv:
	python -m pip install "uv~=0.8.13"

install: install-uv
	uv pip install --system "kedro_datasentinel @ ."

install-lint: install-uv
	uv pip install --system "kedro_datasentinel[lint] @ ."

install-test: install-uv
	uv pip install --system "kedro_datasentinel[test] @ ."

install-scripts: install-uv
	uv pip install --system "kedro_datasentinel[scripts] @ ."

install-pre-commit:
	pre-commit install --install-hooks

install-all: install install-lint install-test install-scripts install-pre-commit

uninstall-pre-commit:
	pre-commit uninstall

package:
	python -m pip install build && python -m build --wheel

lint:
	pre-commit run --all-files --hook-stage manual

type-check:
	pyright kedro_datasentinel

unit-test:
	pytest -m unit
