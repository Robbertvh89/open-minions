"""Executor CLI. Commands are stubs until M1 (loop) and M4 (capacity governor)."""

from typing import Annotated

import typer
from minion_common.enums import CapacityProfile

app = typer.Typer(help="open-minions executor: donate idle Apple Silicon GPU capacity.")


@app.command()
def run(
    server: Annotated[str, typer.Option(help="Minion server base URL")] = "http://localhost:8080",
    profile: Annotated[
        CapacityProfile, typer.Option(help="Capacity profile (SPEC §4.6)")
    ] = CapacityProfile.BALANCED,
) -> None:
    """Poll the server and run subtasks."""
    typer.echo(f"executor loop not implemented yet (server={server}, profile={profile.value})")
    raise typer.Exit(code=1)


@app.command()
def status() -> None:
    """Show current budget, task and contribution."""
    typer.echo("status not implemented yet")
    raise typer.Exit(code=1)
