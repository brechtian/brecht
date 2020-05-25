import json
import logging
import random
import sys
import uuid

from locust import HttpUser, task, between

logger = logging.getLogger('brecht')
logger.setLevel(logging.DEBUG)


class WebsiteUser(HttpUser):
    wait_time = between(0, 3)
    namespace = "development"
    my_sub_stream_id = None
    current_event_seq_num = None
    snapshot_after = None

    @task(2)
    def post_account_event(self):
        self.current_event_seq_num = self.current_event_seq_num + 1
        if self.current_event_seq_num % self.snapshot_after != 0:
            self.post_event()
            logger.info(f"{self.current_event_seq_num} events for entity with id {self.my_sub_stream_id}")
        else:
            self.post_snapshot()

    def post_snapshot(self):
        snapshot_event = {
            "data": {
                "balance": str(888 * self.snapshot_after)
            },
            "eventType": "com.megacorp.AccountBalanceSnapshot",
            "eventId": str(uuid.uuid1()),
            "sequenceNum": self.current_event_seq_num
        }
        snapshot_as_json: str = json.dumps(snapshot_event)
        logger.info(f"Posting snapshot {snapshot_as_json}")
        headers = {'content-type': 'application/json'}
        self.client.post(url=f"/{self.namespace}/snapshot/accounts/{self.my_sub_stream_id}",
                         data=snapshot_as_json,
                         headers=headers,
                         name=f"/{self.namespace}/snapshot/accounts/account-[id]")

    def post_event(self):
        event = {
            "data": {
                "from": "Michael Jackson",
                "amount": 888
            },
            "sequenceNum": self.current_event_seq_num,
            "eventType": "com.megacorp.MoneyDeposited",
            "eventId": str(uuid.uuid1()),
            "tags": ["megacorp"]
        }
        events = [event]
        events_as_json: str = json.dumps(events)
        logger.info(f"Posting {events_as_json}")
        headers = {'content-type': 'application/json'}
        self.client.post(url=f"/{self.namespace}/events/accounts/{self.my_sub_stream_id}",
                         data=events_as_json,
                         headers=headers,
                         name=f"/{self.namespace}/events/accounts/account-[id]")

    @task(4)
    def get_account_events(self):
        self.client.get(f"/{self.namespace}/events/accounts/{self.my_sub_stream_id}",
                        name=f"/{self.namespace}/events/accounts/account-[id]")

    @task(1)
    def get_wrong_namespace(self):
        wrong_namespace = "unknown_namespace"
        with self.client.get(f"/{wrong_namespace}/events/accounts/{self.my_sub_stream_id}",
                             name=f"/{wrong_namespace}/events/accounts/account-[id]",
                             catch_response=True) as response:
            if response.status_code == 404:
                response.success()

    def on_start(self):
        self.my_sub_stream_id = f"account-{str(random.randint(0, sys.maxsize))}"
        self.current_event_seq_num = 0
        self.snapshot_after = 20
        """ on_start is called when a User starts before any task is scheduled """
        self.create_namespaces()

    def create_namespaces(self):
        self.client.post(f"/{self.namespace}", name=f"/{self.namespace}")
