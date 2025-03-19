import click
import logging

LOGGING_LEVELS = {
    0: logging.NOTSET,
    1: logging.DEBUG,
    2: logging.INFO,
    3: logging.WARN,
    4: logging.ERROR,
}  #: a mapping of `verbose` option counts to logging levels


class Config(object):
    """An information object to pass data between CLI functions."""

    def __init__(self):
        """Create a new instance."""
        pass


pass_config = click.make_pass_decorator(Config, ensure=True)


@click.group()
@click.option("--verbose", "-v", count=True, help="Enable verbose output.")
@click.pass_context
def cli(ctx, verbose: int):
    """
    Logging levels:
        0: NOTSET
        1: DEBUG
        2: INFO
        3: WARN
        4: ERROR
    """
    ctx.obj = Config()

    # Use the verbosity count to determine the logging level...
    if verbose > 0:
        logging.basicConfig(
            level=LOGGING_LEVELS[verbose]
            if verbose in LOGGING_LEVELS
            else logging.DEBUG
        )
        click.echo(
            click.style(
                f"Verbose logging is enabled. "
                f"(LEVEL={logging.getLogger().getEffectiveLevel()})",
                fg="yellow",
            )
        )


################
# Carbon Arc CLI
################
@cli.group()
@click.pass_obj
def package(ctx):
    """Package commands"""
    pass


if __name__ == "__main__":
    cli()
