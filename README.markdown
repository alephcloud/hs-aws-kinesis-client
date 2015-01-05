### Kinesis Producer/Consumer Client

This package extends the facilities provided in
[aws-kinesis](http://hackage.haskell.org/package/aws-kinesis) with a
producer and consumer client.

The producer client is to be used to enqueue and balance writes across
multiple shards according to a batching policy. This is necessary to
achieve high throughput.

The consumer client is for reading records off of a Kinesis
stream. Please note that Kinesis does not yet support long polling, so
the consumer client is a labor of love and has thus far required a
fair amount of tweaking to avoid rate-limiting errors---if any occur
during your use of it, please file a report using the issue tracker.


#### Consumer Client CLI

Also included is a minimal command line interface to the consumer
client.
