from os import (
    environ,
    path,
)

from aws_cdk import App

from builder import DatalakeBuilder
from standalone.extra_policies import ExtraPoliciesStack

app: App = App()


# =============================================================================
# DEFAULT DATALAKE STACKS ARE BUILT HERE
#
# it includes:
#   - datalake buckets
#   - datalake glue databases
#   - datalake glue crawlers in raw layer
#   - datalake vpc (optional)
#   - datalake sns topic
#   - datalake pipelines according to the config.yml files in the pipelines folder
# =============================================================================

root = path.dirname(path.abspath(__file__))

datalake = DatalakeBuilder(
    scope=app,
    lake_name="Example Lake",
    region=environ["AWS_DEFAULT_REGION"],
    account_id=environ["AWS_ACCOUNT_ID"],
    env=environ["ENVIRONMENT_STAGE"],
    lake_domains=["example"],
    enable_vpc=False,
    sns_subscriptions=[
        {"protocol": "email", "endpoint": "example@example.com"}
    ],
    pipelines_path=path.join(root, "pipelines"),
    tags={"example1": "value1", "example2": "value2"},
).build()

# =============================================================================
# ADDITIONAL STANDALONE STACKS CAN BE ADDED HERE
#
#  datalake attribues can be accessed via datalake.<attribute>
#
#  examples:
#   - datalake.bucket_set.get(domains=["example"], layers=["raw"])[0].name.value
#   - datalake.vpc
#   - datalake.sns_topic
# =============================================================================

ExtraPoliciesStack(app, "extra-policies-stack")

# =============================================================================
# END OF ADDITIONAL STANDALONE STACKS
# =============================================================================
app.synth()
