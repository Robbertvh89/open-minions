"""`minion` entry point (commands land in step 3)."""

import typer

app = typer.Typer(no_args_is_help=True, add_completion=False, help="open-minions submitter CLI")
