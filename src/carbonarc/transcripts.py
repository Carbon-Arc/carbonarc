from typing import Optional

from carbonarc.utils.client import BaseAPIClient


class TranscriptAPIClient(BaseAPIClient):
    """Client for the Carbon Arc Transcripts API.

    Provides access to expert interview transcripts. Use this client to browse
    available transcripts, purchase individual ones, and download the purchased
    files.

    Access is gated by client entitlement — contact Carbon Arc to enable
    Transcripts for your account.
    """

    def __init__(self, token: str, host: str, version: str):
        super().__init__(token=token, host=host, version=version)
        self._base_url = f"{host.rstrip('/')}/{version}/transcripts"

    def list_transcripts(
        self,
        ticker: Optional[str] = None,
        entity: Optional[str] = None,
        transcript_type: Optional[str] = None,
        region: Optional[str] = None,
        search: Optional[str] = None,
        interview_date_from: Optional[str] = None,
        interview_date_to: Optional[str] = None,
        sort_by: str = "published_at",
        order: str = "desc",
        page: int = 1,
        size: int = 20,
    ) -> dict:
        """List available transcripts for your account.

        Returns transcripts your client is entitled to browse. Each item
        includes an ``is_purchased`` flag indicating whether the calling user
        has already purchased it.

        Args:
            ticker: Filter by ticker symbol (entity label).
            entity: Filter by any entity label.
            transcript_type: Filter by transcript type (e.g. ``"expert_interview"``).
            region: Filter by region (e.g. ``"North America"``).
            search: Search in title and description.
            interview_date_from: ISO date string lower bound, inclusive (e.g. ``"2024-01-01"``).
            interview_date_to: ISO date string upper bound, inclusive (e.g. ``"2024-12-31"``).
            sort_by: Sort field — ``"published_at"`` (default), ``"title"``, or ``"interview_date"``.
            order: Sort direction — ``"desc"`` (default) or ``"asc"``.
            page: Page number, 1-indexed (default ``1``).
            size: Page size, 1–100 (default ``20``).

        Returns:
            Dict with ``transcripts`` (list), ``total`` (int), ``page`` (int), and ``size`` (int).
        """
        params: dict = {"sort_by": sort_by, "order": order, "page": page, "size": size}
        for key, val in {
            "ticker": ticker,
            "entity": entity,
            "transcript_type": transcript_type,
            "region": region,
            "search": search,
            "interview_date_from": interview_date_from,
            "interview_date_to": interview_date_to,
        }.items():
            if val is not None:
                params[key] = val
        return self._get(self._base_url, params=params)

    def get_transcript(self, transcript_id: str) -> dict:
        """Get metadata for a single transcript.

        Returns full details including ontology entity tags and whether the
        calling user has purchased this transcript.

        Args:
            transcript_id: UUID of the transcript.

        Returns:
            Dict with transcript metadata, ``has_pdf`` flag (whether a PDF
            version is available), and ``is_purchased`` flag.
        """
        return self._get(f"{self._base_url}/{transcript_id}")

    def purchase_transcript(self, transcript_id: str) -> dict:
        """Purchase a transcript.

        Deducts tokens from the calling user's balance. Returns the purchase
        record including price and status. Raises a 402 if the balance is
        insufficient and a 409 if already purchased.

        Args:
            transcript_id: UUID of the transcript to purchase.

        Returns:
            Dict with purchase details (``id``, ``transcript_id``, ``price``, ``status``, ``created_at``).
        """
        return self._post(f"{self._base_url}/{transcript_id}/purchase")

    def download_transcript(self, transcript_id: str, fmt: str = "txt") -> dict:
        """Get a presigned download URL for a purchased transcript.

        The URL expires in 15 minutes. Also records the download for audit
        purposes.

        Args:
            transcript_id: UUID of the purchased transcript.
            fmt: File format — ``"txt"`` (default) or ``"pdf"``.

        Returns:
            Dict with ``url`` (presigned S3 URL), ``expires_in`` (seconds), and ``format``.
        """
        return self._get(
            f"{self._base_url}/{transcript_id}/download", params={"fmt": fmt}
        )
