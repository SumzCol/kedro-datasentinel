import re

import click


def validate_version(ctx, param, value) -> str:
    # Updated regex to accept standard versions (x.y.z) and pre-release versions (alpha, beta, rc)
    if not re.match(r"^\d{1,}\.\d{1,}\.\d{1,}((a|alpha|b|beta|rc|c)\d+)?$", value):
        raise ValueError(
            f"Version '{value}' is not in the format x.y.z or x.y.z(a|alpha|b|beta|rc)n"
        )

    return value


@click.command()
@click.option(
    "--change-log-file",
    "-f",
    type=click.Path(exists=True),
    required=True,
    help="Path to the change log file",
)
@click.option(
    "--version",
    "-v",
    type=str,
    required=True,
    help="Version in the format x.y.z or x.y.z(a|alpha|b|beta|rc).n to extract release notes for",
    callback=validate_version,
)
@click.option(
    "--output-file",
    "-o",
    type=click.Path(),
    required=True,
    help="Path to the output file to write the release notes to",
)
def extract_release_notes(change_log_file: str, version: str, output_file: str) -> None:
    with open(change_log_file) as file:
        lines = file.readlines()

    start = False
    release_notes = []

    for line in lines:
        if line.startswith(f"# {version}") and not start:
            start = True
            continue

        if line.startswith("# ") and start:
            break

        if start:
            release_notes.append(line)

    if not release_notes:
        raise ValueError(f"Could not find release notes for version '{version}'.")

    result = "".join(release_notes).strip()

    with open(output_file, "w") as file:
        file.write(result)


if __name__ == "__main__":
    extract_release_notes()
