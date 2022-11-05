#!/usr/bin/env python3

import aws_cdk as cdk

from aws_challenge.aws_challenge_stack import AwsChallengeStack


app = cdk.App()
AwsChallengeStack(app, "aws-challenge")

app.synth()
