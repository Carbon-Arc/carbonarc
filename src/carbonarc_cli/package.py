import click
from typing import Optional


class Package(object):
    def version(self):
        from pyca import __version__

        click.echo(click.style(f"{__version__}", bold=True, italic=True))

    def release(self, version: Optional[str] = None):
        from pyca.utils.release import release as release_package

        release_package(version)

    def docs(self):
        from pyca.utils.release import docs as package_docs

        package_docs()
