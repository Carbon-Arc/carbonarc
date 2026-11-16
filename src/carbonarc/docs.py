import io
import os
import posixpath
import re
import zipfile
from typing import Callable, Dict, Optional
from urllib.parse import quote

import requests

from carbonarc.utils.client import BaseAPIClient

SECTIONS = ("platform", "reference", "developers", "tutorials")
ASSET_TIMEOUT_SECONDS = 60


class DocsAPIClient(BaseAPIClient):
    """
    A client for downloading the published Carbon Arc documentation
    (docs.carbonarc.ai) as Markdown.

    The bundle is rebuilt on every docs deploy, so it matches the live site.
    """

    def __init__(
        self,
        token: str,
        host: str = "https://api.carbonarc.co",
        version: str = "v2",
    ):
        """
        Initialize DocsAPIClient with an authentication token and user agent.

        Args:
            token: The authentication token to be used for requests.
            host: The base URL of the Carbon Arc API.
            version: The API version to use.
        """
        super().__init__(token=token, host=host, version=version)

        self.base_docs_url = self._build_base_url("docs")

    def get_manifest(self) -> dict:
        """
        List the published docs in the download bundle.

        Returns:
            dict: ``count``, per-section counts, one item per doc (``path``,
            ``section``, ``title``, ``size``, ``sha256``), and ``assets``: the
            images (``static/img/...``) and PDFs (``static/pdf/...``) the docs
            reference, which are not in the bundle.
        """
        return self._get(f"{self.base_docs_url}/manifest")

    def get_all(self) -> bytes:
        """
        Retrieve every published doc's Markdown (md/mdx) source as zip bytes.

        Paths inside the zip are ``docs/<section>/...``, where section is one
        of ``platform``, ``reference``, ``developers`` or ``tutorials``.

        Returns:
            bytes: The zip archive.

        Raises:
            requests.exceptions.HTTPError: If the request fails.
        """
        return self._stream(f"{self.base_docs_url}/bundle").content

    def download_all(
        self,
        directory: Optional[str] = None,
        include_images: bool = False,
        include_pdfs: bool = False,
    ) -> str:
        """
        Download every published doc to ``<directory>/docs/<section>/``.

        Overwrites files at the same paths; files from an earlier download that
        are no longer published are left in place.

        Image and PDF links point at docs.carbonarc.ai, so they open while
        online. ``include_images`` also saves the images the docs use (~500 MB)
        under ``<directory>/static/img/``, and ``include_pdfs`` the PDF reports
        they link to (~120 MB) under ``<directory>/static/pdf/``; links to saved
        files point at the local copies, so they open offline.

        Args:
            directory: The folder to write into. Defaults to the current
                directory and is created if it does not exist.
            include_images: Also download the images the docs use.
            include_pdfs: Also download the PDF reports the docs link to.

        Returns:
            str: The absolute path of ``directory``.

        Raises:
            requests.exceptions.HTTPError: If a request fails.
            OSError: If there are file system errors (permissions, disk space, etc.).
        """
        return self._download(None, directory, include_images, include_pdfs)

    def download_folder(
        self,
        folder: str,
        directory: Optional[str] = None,
        include_images: bool = False,
        include_pdfs: bool = False,
    ) -> str:
        """
        Download one docs section to a local folder.

        Same as :meth:`download_all`, limited to one section and the images and
        PDFs it uses.

        Args:
            folder: ``"platform"``, ``"reference"``, ``"developers"`` or ``"tutorials"``.
            directory: The folder to write into. Defaults to the current
                directory and is created if it does not exist.
            include_images: Also download the images this section uses.
            include_pdfs: Also download the PDF reports this section links to.

        Returns:
            str: The absolute path of ``directory``.

        Raises:
            ValueError: If ``folder`` is not one of the four sections.
            requests.exceptions.HTTPError: If a request fails.
            OSError: If there are file system errors (permissions, disk space, etc.).
        """
        if folder not in SECTIONS:
            raise ValueError(f"folder must be one of {', '.join(SECTIONS)}; got {folder!r}")
        return self._download(folder, directory, include_images, include_pdfs)

    def _download(
        self,
        folder: Optional[str],
        directory: Optional[str],
        include_images: bool,
        include_pdfs: bool,
    ) -> str:
        output_dir = os.path.abspath(directory if directory is not None else ".")
        prefix = f"docs/{folder}/" if folder else "docs/"
        # Fetched before anything is written, so a failed bundle request writes nothing.
        bundle_bytes = self.get_all()

        asset_dirs = tuple(
            asset_dir
            for asset_dir, wanted in (
                ("static/img/", include_images),
                ("static/pdf/", include_pdfs),
            )
            if wanted
        )
        saved: Dict[str, str] = {}  # site URL -> local static/... path
        if asset_dirs:
            assets = [
                asset
                for asset in self.get_manifest().get("assets", [])
                if asset.get("path", "").startswith(asset_dirs)
                and (folder is None or folder in asset.get("sections", []))
            ]
            # Public files on the docs site: a separate session, so no API token.
            with requests.Session() as session:
                for asset in assets:
                    response = session.get(asset["url"], timeout=ASSET_TIMEOUT_SECONDS)
                    response.raise_for_status()
                    _write_file(output_dir, asset["path"], response.content)
                    saved[asset["url"]] = asset["path"]

        link_locally = _local_linker(saved) if saved else None
        with zipfile.ZipFile(io.BytesIO(bundle_bytes)) as bundle:
            for name in bundle.namelist():
                if name.startswith(prefix) and not name.endswith("/"):
                    data = bundle.read(name)
                    if link_locally:
                        data = link_locally(data, name)
                    _write_file(output_dir, name, data)

        return output_dir


def _local_linker(saved: Dict[str, str]) -> Callable[[bytes, str], bytes]:
    """Return a function that points a doc's links to ``saved`` (site URL ->
    static/... path) at the local copies, relative to the doc."""
    # Longest first, so a URL that is a prefix of another never matches early.
    saved_url_re = re.compile("|".join(map(re.escape, sorted(saved, key=len, reverse=True))))

    def link_locally(data: bytes, doc_path: str) -> bytes:
        doc_dir = posixpath.dirname(doc_path)
        text = saved_url_re.sub(
            lambda m: quote(posixpath.relpath(saved[m.group(0)], doc_dir)),
            data.decode("utf-8"),
        )
        return text.encode("utf-8")

    return link_locally


def _write_file(output_dir: str, relative_path: str, data: bytes) -> None:
    path = os.path.abspath(os.path.join(output_dir, relative_path))
    # Reject zip or manifest paths ("../x", "/etc/x") that escape output_dir.
    if path == output_dir or os.path.commonpath([output_dir, path]) != output_dir:
        raise ValueError(f"Refusing to write outside {output_dir}: {relative_path!r}")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "wb") as f:
        f.write(data)
