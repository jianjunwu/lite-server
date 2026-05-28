"""Interactive wizard for lite-server init."""

from __future__ import annotations

import sys
from typing import Any

from lite_server.init.generator import ProjectGenerator, TEMPLATES


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


def _ask_choice(question: str, choices: list[str], default: str = "") -> str:
    print(f"\n{question}")
    for i, choice in enumerate(choices, 1):
        mark = " (default)" if choice == default else ""
        print(f"  {i}. {choice}{mark}")
    answer = _ask("Enter number or name", default)
    # Try number
    try:
        idx = int(answer) - 1
        if 0 <= idx < len(choices):
            return choices[idx]
    except ValueError:
        pass
    # Try exact match
    if answer in choices:
        return answer
    # Fallback to default
    if default:
        return default
    return choices[0]


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

    # Template
    template = _ask_choice(
        "Choose a template:",
        list(TEMPLATES),
        default="empty",
    )

    # Model name
    model_name = _ask("Model name", "my_model")

    print("\n--- Service Configuration ---")
    grpc = _ask_yn("Enable gRPC?", default=True)
    metrics = _ask_yn("Enable Prometheus metrics?", default=True)
    webui = _ask_yn("Enable Web UI?", default=True)

    print("\n--- Inference Features ---")
    batch = _ask_yn("Enable dynamic batching?", default=False)
    stream = _ask_yn("Enable streaming responses?", default=False)

    options: dict[str, Any] = {
        "grpc": grpc,
        "metrics": metrics,
        "webui": webui,
        "batch": batch,
        "stream": stream,
    }

    print(f"\nGenerating project '{project_name}' with template '{template}'...")

    generator = ProjectGenerator(
        project_name=project_name,
        template=template,
        output_dir=output_dir,
        model_name=model_name,
        options=options,
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
