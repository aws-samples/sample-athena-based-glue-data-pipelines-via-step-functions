"""CDK App"""
# pylint: disable=unused-variable
import config as cf
import aws_cdk as _cdk
from base_stacks import LakeStorageStack
from lake_stacks import BasePipeline, LakeStack

from pipeline_stacks import (
    USGHGEmissionDailyPipeline,
    USGHGEmissionMonthlyPipeline,
)

app = _cdk.App()

# Base / Common Stacks

# All S3 Buckets
storage_stack = LakeStorageStack(app, construct_id="storage-stack", env=cf.CDK_ENV)

# Glue DBs
lake_stack = LakeStack(app, construct_id="lake-stack", env=cf.CDK_ENV)
lake_stack.add_dependency(storage_stack)

# Base data Pipelines Stack
base_pipeline_stack = BasePipeline(
    app, construct_id="base-pipeline-stack", env=cf.CDK_ENV
)
base_pipeline_stack.add_dependency(lake_stack)

# Data pipelines
# US GHG Emission Daily ( pipeline )
usghg_emission_daily_stack = USGHGEmissionDailyPipeline(
    app, construct_id="usghg-emission-stack-daily", env=cf.CDK_ENV
)
usghg_emission_daily_stack.add_dependency(base_pipeline_stack)

# US GHG Emission Monthly ( pipeline )
usghg_emission_monthly_stack = USGHGEmissionMonthlyPipeline(
    app, construct_id="usghg-emission-stack-monthly", env=cf.CDK_ENV
)
usghg_emission_monthly_stack.add_dependency(base_pipeline_stack)

app.synth(skip_validation=False)
