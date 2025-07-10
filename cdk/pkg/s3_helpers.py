from typing import Optional

from aws_cdk import aws_s3 as s3

import config as cf

from constructs import Construct


def get_bucket_name(bucket_type: str) -> str:
    """Gets bucket name given type"""
    validate_bucket_type(bucket_type)
    return getattr(cf, f"S3_{bucket_type.upper()}_BUCKET")


def get_bucket_arn(bucket_type: str) -> str:
    """Gets bucket arn given bucket type"""
    bucket_name = get_bucket_name(bucket_type)
    return f"arn:aws:s3:::{bucket_name}"


def get_bucket(
    scope: Construct, bucket_type: str, id_suffix: Optional[str] = None
) -> s3.Bucket:
    """Returns bucket given Lake bucket_type"""
    bucket_name = get_bucket_name(bucket_type)
    construct_id = str(hash(f"{bucket_type}-{id_suffix}"))
    return s3.Bucket.from_bucket_name(
        scope,
        id=construct_id,
        bucket_name=bucket_name,
    )


def validate_bucket_type(bucket_type: str):
    """Validates bucket type"""
    if bucket_type not in (
        "landing",
        "processed",
        "athena",
        "log",
        "glue_assets",
        "audit",
    ):
        raise ValueError(
            "Bucket type must be in 'landing', 'processed', "
            "'athena', 'log', 'glue_assets'"
        )
