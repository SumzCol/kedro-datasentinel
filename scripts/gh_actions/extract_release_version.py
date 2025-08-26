import ast
import os
import re

import click


def extract_version_from_file(file_path: str) -> str:
    """Extract version from a Python file.

    This function looks for a __version__ variable in the file and returns its value.
    It supports standard versions (x.y.z) as well as alpha, beta, and release candidate versions.

    Args:
        file_path: Path to the Python file containing the version

    Returns:
        The version string

    Raises:
        ValueError: If the version cannot be found in the file
    """
    with open(file_path) as file:
        content = file.read()

    # Try to find version using AST parsing (safer)
    try:
        tree = ast.parse(content)
        for node in ast.walk(tree):
            if isinstance(node, ast.Assign):
                for target in node.targets:
                    if isinstance(target, ast.Name) and target.id == "__version__":
                        if isinstance(node.value, ast.Constant):
                            return node.value.value
    except SyntaxError:
        pass  # Fall back to regex if AST parsing fails

    # Fall back to regex pattern matching
    version_pattern = r'__version__\s*=\s*[\'"]([^\'"]*)[\'"]'
    match = re.search(version_pattern, content)

    if match:
        return match.group(1)

    raise ValueError(f"Could not find version in file: {file_path}")


@click.command()
@click.option(
    "--file-path",
    "-f",
    type=click.Path(exists=True),
    required=True,
    help="Path to the Python file containing the version (usually __init__.py)",
)
def extract_release_version(file_path: str) -> None:
    """Extract the release version from a Python file.

    This tool extracts the __version__ variable from a Python file and outputs it.
    It supports standard versions (x.y.z) as well as alpha, beta, and release candidate versions.
    """
    version = extract_version_from_file(file_path)

    # Validate the version format
    if not re.match(r"^\d{1,}\.\d{1,}\.\d{1,}((a|alpha|b|beta|rc|c)\d+)?$", version):
        raise ValueError(f"Version '{version}' is not in a valid format")

    env_file = os.getenv("GITHUB_ENV")
    if env_file is not None:
        with open(env_file, "a") as file:
            file.write(f"DATA_SENTINEL_VERSION={version}\n")
    else:
        raise FileNotFoundError("GITHUB_ENV environment variable is not set.")


if __name__ == "__main__":
    extract_release_version()
