## HTTP 1.1 and JSON API

FlixDb offers a HTTP 1.1 / JSON API. Though, we see it more as a way for 
users to familiarize themselves with FlixDb and, 
in production, we'd like users to use
the gRPC clients, the HTTP API is still very robust and is capable of powering a business.

### Post an Event

The following request posts a list of events (can be a list
with only one item) to the 'account-42' entity of the 'accounts' stream in the 
'default' namespace. 

```
curl -H "Content-Type: application/json" --data '[{"eventId": "3a804c3b-cb50-4fc7-87c3-eb8bb01db6a2","eventType": "com.good-startup.AccountCreated", "sequenceNum" : 0, "data" : {"accountId": "42", "owner": "John Doe", "monthlyAmount": 888}, "stream": "account-events"}]' http://localhost:8080/default/events/accounts/account-42 
```

The result is 200 OK.

This endpoint is idempotent (based on the namespace, eventId and name of the stream). 

### Get the events

```
curl -XGET http://localhost:8080/default/events/accounts/account-42
```


#### Result

```
[
  {
    "data": {
      "accountId": "42",
      "monthlyAmount": 888,
      "owner": "John Doe"
    },
    "entityId": "account-42",
    "eventId": "3a804c3b-cb50-4fc7-87c3-eb8bb01db6a2",
    "eventType": "com.good-startup.AccountCreated",
    "sequenceNum": 0,
    "stream": "accounts",
    "tags": [],
    "timestamp": 1587442659639
  }
]
```