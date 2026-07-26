"""Interactive wizard for lite-server init."""

from __future__ import annotations

import sys

from lite_server.init.generator import ProjectGenerator


def _ask(question: str, default: str = "") -> str:
    if default:
        prompt = f"{question} [{default}]: "
    else:
        prompt = f"{question}: "
    try:
        answer = input(prompt).strip()
    except (EOFError, KeyboardInterrupt):
        print("\nAborted.", file=sys.stderr)
        sys.exit(1)
    return answer if answer else default


def _ask_yn(question: str, default: bool = True) -> bool:
    default_str = "Y/n" if default else "y/N"
    answer = _ask(question, default_str).lower()
    if not answer or answer == default_str.lower():
        return default
    return answer.startswith("y")


def run_wizard(output_dir: str = ".") -> None:
    """Run the interactive project initialization wizard."""
    print("=" * 50)
    print("  lite-server init - Project Wizard")
    print("=" * 50)
    print()

    # Project name
    project_name = _ask("Project name")
    if not project_name:
        print("Project name is required.", file=sys.stderr)
        sys.exit(1)

    # Model name
    model_name = _ask("Model name", "my_model")

    # --- Summary & confirmation ---
    print("\n" + "=" * 50)
    print("  Summary")
    print("=" * 50)
    print(f"  Project:    {project_name}")
    print(f"  Model:      {model_name}")
    print("=" * 50)
    confirmed = _ask_yn("Create project?", default=True)
    if not confirmed:
        print("Aborted.", file=sys.stderr)
        sys.exit(0)

    print(f"\nGenerating project '{project_name}'...")

    generator = ProjectGenerator(
        project_name=project_name,
        output_dir=output_dir,
        model_name=model_name,
    )
    try:
        root = generator.generate()
    except FileExistsError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

    print(f"\nCreated project at: {root}")
    print(f"\nNext steps:")
    print(f"  cd {project_name}")
    print(f"  lite-server serve --config server.yaml")
    print(f"  # In another terminal:")
    print(f"  python test_request.py")
