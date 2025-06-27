import os
import logging
from io import BytesIO
from typing import Optional
import base64

from carbonarc.base.client import BaseAPIClient
from carbonarc.base.utils import is_valid_date

log = logging.getLogger(__name__)


class DataAPIClient(BaseAPIClient):
    """
    A client for interacting with the Carbon Arc Data API.
    """

    def __init__(
        self,
        token: str,
        host: str = "https://platform.carbonarc.co",
        version: str = "v2",
    ):
        """
        Initialize DataAPIClient with an authentication token and user agent.
        
        Args:
            token: The authentication token to be used for requests.
            host: The base URL of the Carbon Arc API.
            version: The API version to use.
        """
        super().__init__(token=token, host=host, version=version)

        self.base_data_url = self._build_base_url("library")

    def get_datasets(
        self,
    ) -> dict:
        url = f"{self.base_data_url}/data"

        return self._get(url)

    def get_dataset_information(self, data_identifier: str) -> dict:
        """
        Get the information for a specific dataset from the Carbon Arc API.
        
        Args:
            data_identifier (str): The identifier of the data to retrieve information for.
            
        Returns:
            dict: A dictionary containing the information for the specified dataset.
        """
        endpoint = f"data/{data_identifier}/information"
        url = f"{self.base_data_url}/{endpoint}"

        return self._get(url)

    def get_data_manifest(
        self,
        data_identifier: str,
        created_since: Optional[str] = None,
        updated_since: Optional[str] = None,
    ) -> dict:
        """
        Get the manifest for a specific data identifier from the Carbon Arc API.
        
        Args:
            data_identifier (str): The identifier of the data to retrieve manifest for.
            created_since (Optional[str]): The filter for created timestamp. Format is YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS.
            updated_since (Optional[str]): The filter by updated timestamp, modification_time field. Format is YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS.
            
        Returns:
            dict: A dictionary containing the manifest for the specified data identifier.
        """
        endpoint = f"data/{data_identifier}/manifest"
        url = f"{self.base_data_url}/{endpoint}"
        params = {}
        if created_since:
            # validate created_since format
            if not is_valid_date(created_since):
                raise ValueError(
                    "created_since must be in YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS format."
                )
            params["created_since"] = created_since
        if updated_since:
            # validate updated_since format
            if not is_valid_date(updated_since):
                raise ValueError(
                    "updated_since must be in YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS format."
                )
            params["updated_since"] = updated_since
        return self._get(url, params=params)
    
    def buy_data(self, order: dict) -> dict:
        """
        Buy data from the Carbon Arc API.
        
        Args:
            order (dict): The order to buy data for.
            
        Returns:
            dict: A dictionary containing the information for the specified order.
        """
        endpoint = "data/buy"
        url = f"{self.base_data_url}/{endpoint}"

        return self._post(url, json=order)
    
    def get_order_details(self, order_id: str) -> dict:
        """
        Get the details of an order from the Carbon Arc API.

        Args:
            order_id (str): The ID of the order to get details for.

        Returns:
            dict: A dictionary containing the details of the order.
        """
        endpoint = f"data/order/{order_id}"
        url = f"{self.base_data_url}/{endpoint}"

        return self._get(url)
    
    def download_file(self, file_id: str) -> dict:
        """
        Download a data file from the Carbon Arc API.

        Args:
            file_id (str): The ID of the file to download.

        Returns:
            dict: A dictionary containing the file.
        """
        endpoint = f"data/files/{file_id}"
        url = f"{self.base_data_url}/{endpoint}"

        return self._get(url)
    
    def __stream_data(
        self,
        url: str,
        chunk_size: int = 1024 * 1024 * 250,  # 250MB
    ):
        """
        Download a file stream from the Carbon Arc API.
        
        Args:
            url (str): The URL of the file to download.
            chunk_size (int): The size of each chunk to download.
            
        Returns:
            generator: A generator yielding the raw stream of the file.
        """
        response = self.request_manager.get_stream(url)
        for chunk in response.iter_content(chunk_size=chunk_size):
            yield chunk

    def download_data_to_file(
        self, url: str, output_file: str, chunk_size: int = 1024 * 1024 * 250
    ):
        """
        Download data for a specific data identifier and save it to a file.
        
        Args:
            url (str): The URL of the file to download.
            output_file (str): The path to the file where the data should be saved.
            chunk_size (int): The size of each chunk to download.
            
        Returns:
            str: The path to the downloaded file.
        """
        # check if output_file directory exists
        output_dir = os.path.dirname(output_file)
        if not os.path.exists(output_dir):
            raise FileNotFoundError(f"Output directory {output_dir} does not exist.")

        with open(output_file, "wb") as f:
            for chunk in self.__stream_data(url, chunk_size):
                f.write(chunk)

    def download_data_to_s3(
        self,
        s3_client,
        file_url: str,
        s3_bucket: str,
        s3_key_prefix: str,
        chunk_size: int = 5 * 1024 * 1024,  # Default to 5MB
    ):
        log.info(f"Downloading file {file_url} to S3...")

        # Ensure chunk size is at least 5MB (AWS requirement for multipart uploads)
        if chunk_size < 5 * 1024 * 1024:
            chunk_size = 5 * 1024 * 1024
            log.info(
                "Chunk size adjusted to 5MB to meet AWS minimum part size requirement"
            )

        # Make the request
        response = self.request_manager.get_stream(file_url)
        response.raise_for_status()

        # Extract filename from response headers
        filename = (
            response.headers["Content-Disposition"].split("filename=")[1].strip('"')
        )

        # Create the full S3 key (path + filename)
        s3_key = f"{s3_key_prefix.rstrip('/')}/{filename}"

        # Check if file is small enough for direct upload
        content_length = int(response.headers.get("Content-Length", 0))

        # If file is small (less than 10MB) or content length is unknown, use simple upload
        if content_length > 0 and content_length < 10 * 1024 * 1024:
            log.warning(f"File is small ({content_length} bytes), using simple upload")
            content = response.content
            s3_client.put_object(Bucket=s3_bucket, Key=s3_key, Body=content)
            log.info(f"File uploaded successfully to s3://{s3_bucket}/{s3_key}")
            return f"s3://{s3_bucket}/{s3_key}"

        # For larger files, use multipart upload
        log.info(f"Initiating multipart upload to s3://{s3_bucket}/{s3_key}")
        multipart_upload = s3_client.create_multipart_upload(
            Bucket=s3_bucket, Key=s3_key
        )

        upload_id = multipart_upload["UploadId"]
        parts = []
        part_number = 1

        try:
            # Use a buffer to collect chunks until we have at least 5MB
            buffer = BytesIO()
            buffer_size = 0

            for chunk in response.iter_content(
                chunk_size=1024 * 1024
            ):  # Read in 1MB chunks
                if not chunk:
                    continue

                # Add the chunk to our buffer
                buffer.write(chunk)
                buffer_size += len(chunk)

                # If we have at least 5MB (or this is the last chunk), upload the part
                if buffer_size >= chunk_size:
                    # Reset buffer position to beginning for reading
                    buffer.seek(0)

                    # Upload the part
                    part = s3_client.upload_part(
                        Bucket=s3_bucket,
                        Key=s3_key,
                        PartNumber=part_number,
                        UploadId=upload_id,
                        Body=buffer.read(),
                    )

                    # Add the part info to our parts list
                    parts.append({"PartNumber": part_number, "ETag": part["ETag"]})

                    log.info(f"Uploaded part {part_number} ({buffer_size} bytes)")
                    part_number += 1

                    # Reset the buffer
                    buffer = BytesIO()
                    buffer_size = 0

            # Upload any remaining data as the final part (can be less than 5MB)
            if buffer_size > 0:
                buffer.seek(0)
                part = s3_client.upload_part(
                    Bucket=s3_bucket,
                    Key=s3_key,
                    PartNumber=part_number,
                    UploadId=upload_id,
                    Body=buffer.read(),
                )

                parts.append({"PartNumber": part_number, "ETag": part["ETag"]})

                log.info(f"Uploaded final part {part_number} ({buffer_size} bytes)")

            # Complete the multipart upload only if we have parts
            if parts:
                s3_client.complete_multipart_upload(
                    Bucket=s3_bucket,
                    Key=s3_key,
                    UploadId=upload_id,
                    MultipartUpload={"Parts": parts},
                )

                log.info(f"File uploaded successfully to s3://{s3_bucket}/{s3_key}")
            else:
                # No parts were uploaded, likely an empty file
                s3_client.abort_multipart_upload(
                    Bucket=s3_bucket, Key=s3_key, UploadId=upload_id
                )

                # Upload an empty file instead
                s3_client.put_object(Bucket=s3_bucket, Key=s3_key, Body=b"")
                log.warning(f"Empty file uploaded to s3://{s3_bucket}/{s3_key}")

            return f"s3://{s3_bucket}/{s3_key}"

        except Exception as e:
            # Abort the multipart upload if something goes wrong
            s3_client.abort_multipart_upload(
                Bucket=s3_bucket, Key=s3_key, UploadId=upload_id
            )
            log.error(f"Multipart upload aborted due to: {str(e)}")
            raise

    def download_data_to_azure(
        self,
        blob_service_client,
        file_url: str,
        container_name: str,
        blob_prefix: str,
        chunk_size: int = 4 * 1024 * 1024,  # Default to 4MB (Azure recommendation)
    ):
        log.info(f"Downloading file {file_url} to Azure Blob Storage...")

        # Ensure chunk size is at least 4MB (Azure recommendation for block blobs)
        if chunk_size < 4 * 1024 * 1024:
            chunk_size = 4 * 1024 * 1024
            log.info(
                "Chunk size adjusted to 4MB for optimal Azure Blob Storage performance"
            )

        # Make the request
        response = self.request_manager.get_stream(file_url)
        response.raise_for_status()

        # Extract filename from response headers
        filename = (
            response.headers["Content-Disposition"].split("filename=")[1].strip('"')
        )

        # Create the full blob path (prefix + filename)
        blob_name = f"{blob_prefix.rstrip('/')}/{filename}"

        # Check if file is small enough for direct upload
        content_length = int(response.headers.get("Content-Length", 0))

        # If file is small (less than 10MB) or content length is unknown, use simple upload
        if content_length > 0 and content_length < 10 * 1024 * 1024:
            log.warning(f"File is small ({content_length} bytes), using simple upload")
            content = response.content
            
            # Get blob client
            blob_client = blob_service_client.get_blob_client(
                container=container_name, blob=blob_name
            )
            
            # Upload the content
            blob_client.upload_blob(content, overwrite=True)
            log.info(f"File uploaded successfully to azure://{container_name}/{blob_name}")
            return f"azure://{container_name}/{blob_name}"

        # For larger files, use block blob upload
        log.info(f"Initiating block blob upload to azure://{container_name}/{blob_name}")
        
        # Get blob client
        blob_client = blob_service_client.get_blob_client(
            container=container_name, blob=blob_name
        )

        block_list = []
        block_number = 0

        try:
            # Use a buffer to collect chunks until we have the required size
            buffer = BytesIO()
            buffer_size = 0

            for chunk in response.iter_content(
                chunk_size=1024 * 1024
            ):  # Read in 1MB chunks
                if not chunk:
                    continue

                # Add the chunk to our buffer
                buffer.write(chunk)
                buffer_size += len(chunk)

                # If we have enough data, upload the block
                if buffer_size >= chunk_size:
                    # Reset buffer position to beginning for reading
                    buffer.seek(0)

                    # Generate block ID (must be base64 encoded)
                    block_id = base64.b64encode(f"block-{block_number:06d}".encode()).decode()

                    # Upload the block
                    blob_client.stage_block(block_id, buffer.read())

                    # Add the block ID to our list
                    block_list.append(block_id)

                    log.info(f"Uploaded block {block_number} ({buffer_size} bytes)")
                    block_number += 1

                    # Reset the buffer
                    buffer = BytesIO()
                    buffer_size = 0

            # Upload any remaining data as the final block
            if buffer_size > 0:
                buffer.seek(0)
                block_id = base64.b64encode(f"block-{block_number:06d}".encode()).decode()
                blob_client.stage_block(block_id, buffer.read())
                block_list.append(block_id)

                log.info(f"Uploaded final block {block_number} ({buffer_size} bytes)")

            # Commit the block list only if we have blocks
            if block_list:
                blob_client.commit_block_list(block_list)
                log.info(f"File uploaded successfully to azure://{container_name}/{blob_name}")
            else:
                # No blocks were uploaded, likely an empty file
                blob_client.upload_blob(b"", overwrite=True)
                log.warning(f"Empty file uploaded to azure://{container_name}/{blob_name}")

            return f"azure://{container_name}/{blob_name}"

        except Exception as e:
            log.error(f"Azure blob upload failed due to: {str(e)}")
            raise

    def download_data_to_gcp(
        self,
        storage_client,
        file_url: str,
        bucket_name: str,
        blob_prefix: str,
        chunk_size: int = 5 * 1024 * 1024,  # Default to 5MB
    ):
        log.info(f"Downloading file {file_url} to Google Cloud Storage...")

        # Ensure chunk size is at least 5MB (GCP recommendation for resumable uploads)
        if chunk_size < 5 * 1024 * 1024:
            chunk_size = 5 * 1024 * 1024
            log.info(
                "Chunk size adjusted to 5MB for optimal Google Cloud Storage performance"
            )

        # Make the request
        response = self.request_manager.get_stream(file_url)
        response.raise_for_status()

        # Extract filename from response headers
        filename = (
            response.headers["Content-Disposition"].split("filename=")[1].strip('"')
        )

        # Create the full blob path (prefix + filename)
        blob_name = f"{blob_prefix.rstrip('/')}/{filename}"

        # Check if file is small enough for direct upload
        content_length = int(response.headers.get("Content-Length", 0))

        # If file is small (less than 10MB) or content length is unknown, use simple upload
        if content_length > 0 and content_length < 10 * 1024 * 1024:
            log.warning(f"File is small ({content_length} bytes), using simple upload")
            content = response.content
            
            # Get bucket and blob
            bucket = storage_client.bucket(bucket_name)
            blob = bucket.blob(blob_name)
            
            # Upload the content
            blob.upload_from_string(content)
            log.info(f"File uploaded successfully to gs://{bucket_name}/{blob_name}")
            return f"gs://{bucket_name}/{blob_name}"

        # For larger files, use resumable upload
        log.info(f"Initiating resumable upload to gs://{bucket_name}/{blob_name}")
        
        # Get bucket and blob
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)

        try:
            # Start resumable upload
            transport = storage_client._http
            url = blob._get_upload_url(transport)
            
            # Use a buffer to collect chunks
            buffer = BytesIO()
            buffer_size = 0
            total_uploaded = 0

            for chunk in response.iter_content(
                chunk_size=1024 * 1024
            ):  # Read in 1MB chunks
                if not chunk:
                    continue

                # Add the chunk to our buffer
                buffer.write(chunk)
                buffer_size += len(chunk)

                # If we have enough data, upload the chunk
                if buffer_size >= chunk_size:
                    # Reset buffer position to beginning for reading
                    buffer.seek(0)
                    chunk_data = buffer.read()

                    # Upload the chunk
                    blob._do_upload_chunk(transport, url, chunk_data, total_uploaded)
                    total_uploaded += len(chunk_data)

                    log.info(f"Uploaded chunk ({len(chunk_data)} bytes), total: {total_uploaded} bytes")

                    # Reset the buffer
                    buffer = BytesIO()
                    buffer_size = 0

            # Upload any remaining data as the final chunk
            if buffer_size > 0:
                buffer.seek(0)
                chunk_data = buffer.read()
                blob._do_upload_chunk(transport, url, chunk_data, total_uploaded)
                total_uploaded += len(chunk_data)

                log.info(f"Uploaded final chunk ({len(chunk_data)} bytes), total: {total_uploaded} bytes")

            # Finalize the upload
            blob._do_finalize_upload(transport, url, total_uploaded)
            log.info(f"File uploaded successfully to gs://{bucket_name}/{blob_name}")

            return f"gs://{bucket_name}/{blob_name}"

        except Exception as e:
            log.error(f"Google Cloud Storage upload failed due to: {str(e)}")
            raise

    def get_graphs(self) -> dict:
        raise NotImplementedError("get_graphs is not implemented yet.")

    def get_graph_data(self, data_identifier: str) -> dict:
        raise NotImplementedError("get_graph_data is not implemented yet.")
