install-uv:
	python -m pip install "uv~=0.8.13"

install: install-uv
	uv pip install --system "kedro_datasentinel @ ."

install-pre-commit:
	pre-commit install --install-hooks

uninstall-pre-commit:
	pre-commit uninstall

package:
	python -m pip install build && python -m build --wheel

lint:
	pre-commit run --all-files --hook-stage manual