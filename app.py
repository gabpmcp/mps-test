#!/usr/bin/env python3
import os, aws_cdk as cdk
from stacks.fn_stack import build_stack

app = cdk.App()
build_stack(
    app, "DataTest",
    account=os.getenv("CDK_DEFAULT_ACCOUNT"),
    region=os.getenv("CDK_DEFAULT_REGION"),
)
app.synth()