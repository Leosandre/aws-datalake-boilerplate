from dataclasses import (
    dataclass,
    field,
)
from itertools import product
from typing import (
    Dict,
    List,
    Optional,
)

from builder.model.property.environment import Environment
from builder.model.property.layer import DatalakeLayer
from builder.model.property.name import Name


@dataclass
class DatalakeBucket:
    domain: str
    layer: DatalakeLayer
    name: Name
    uri: str
    arn: str
    database: Name
    database_arn: str
    crawler: Name


@dataclass
class DatalakeBucketSet:
    region: str
    account_id: str
    domains: List[str]
    env: Environment
    buckets: List[DatalakeBucket] = field(default_factory=list)

    def __post_init__(self) -> None:
        layers = [l for l in DatalakeLayer]
        combinations = list(product(self.domains, layers))

        for domain, layer in combinations:
            name = Name(
                f"{domain}-{layer.value}-{self.region}-{self.account_id}",
                self.env,
            )
            database_name = Name(f"{domain}-{layer.value}", self.env)
            crawler_name = Name(f"{domain}-{layer.value}- crawler", self.env)
            database_arn = f"arn:aws:glue:{self.region}:{self.account_id}:database/{database_name.value}"

            self.buckets.append(
                DatalakeBucket(
                    domain=domain,
                    layer=layer,
                    name=name,
                    uri=f"s3://{name.value}",
                    arn=f"arn:aws:s3:::{name.value}",
                    database=database_name,
                    database_arn=database_arn,
                    crawler=crawler_name,
                )
            )

    def get(
        self,
        domains: Optional[List[str]] = None,
        layers: Optional[List[DatalakeLayer]] = None,
    ) -> List[DatalakeBucket]:
        if not domains and not layers:
            return self.buckets

        filtered_buckets = []
        for bucket in self.buckets:
            if (not domains or bucket.domain in domains) and (
                not layers or bucket.layer in layers
            ):
                filtered_buckets.append(bucket)

        return filtered_buckets
