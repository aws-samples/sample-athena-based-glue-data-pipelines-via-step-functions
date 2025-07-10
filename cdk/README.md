# Overview

## Setup

### Install CDK
To install CDK follow https://docs.aws.amazon.com/cdk/latest/guide/getting_started.html#getting_started_install

The installation requires `npm` / `nodejs`

### Install Python deps
To install project deps run `poetry install`.

If you don't have poetry dependency manager installed, see: https://python-poetry.org/docs/

### AWS setup
To run CDK commands, an awscli credentials / configuration will need to be setup for your AWS accounts for `dev`, `beta`, `prod`.

## CDK Commands

To synthesize CloudFormation, run `DEPLOYMENT_STAGE={STAGE} cdk synth --profile profile` in repo root.

To see resource diff / changeset run `DEPLOYMENT_STAGE={STAGE} cdk diff --profile profile` in repo root.

To deploy run `DEPLOYMENT_STAGE={STAGE} cdk deploy --profile profile` in repo root.

## Deploy only specific pipeline tasks:
export CDK_DEFAULT_ACCOUNT="TBU"
export DEPLOYMENT_STAGE="dev"
export CDK_DEFAULT_REGION="us-east-1"

cdk deploy us-ghg-emission-stack

