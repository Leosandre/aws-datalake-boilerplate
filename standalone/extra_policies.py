from aws_cdk import Stack
from aws_cdk import aws_events as events
from constructs import Construct


class ExtraPoliciesStack(Stack):
    """Example of standalone stack with extra resources"""

    def __init__(self, scope: Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)
        self.add_event_bus_policy()

    def add_event_bus_policy(self) -> None:
        bus = events.EventBus.from_event_bus_name(self, "EventBus", "default")
        events.CfnEventBusPolicy(
            self,
            "EventBusPolicy",
            action="events:PutEvents",
            principal="*",
            statement_id="AllowAll",
            event_bus_name=bus.event_bus_name,
        )
