### v0.3.0.1

- [Producer] Write errors to stderr rather than stdout.

- [Consumer CLI] Exit with a failure code (1) when the run is not considered
  successful (i.e.  if a limit was specified, and the CLI failed to retrieve
  that many records).

### v0.3.0.0

All changes were in the Producer client.

- Do not retry messages indefinitely; add support for a configurable retry
  policy.

- Reject messages synchronously which exceed the Kinesis record size limit.

### v0.2.0.3

All changes were in the Consumer CLI.

- Add a --timeout option, which will cause the consumer CLI terminate after a
  specified number of seconds.

- Change the behavior of --save-state to only save the state when a run of the
  consumer CLI was successful (i.e. either no limit was specified, or the
  precise number of items requested was indeed retrieved within the alotted time).


### v0.2.0.2

- Upgrade to newer versions of `aws-general` and `aws-kinesis` which support the
  security token header (necessary for using credentials from EC2 instance
  metadata). The --use-instance-metadata option should now work properly.

- When --restore-state is passed with a non-existent file, it will be ignored;
  if it is passed with a malformed file, an error will be thrown; the old
  behavior was to throw an error in either case.

### v0.2.0.1

All changes were in the Consumer CLI.

- Add switch to get credentials from EC2 instance metadata,
  --use-instance-metadata

- Make --limit optional to consume indefinitely

- Allow custom region with --region

- If the CLI is terminated for any reason, before shutting down it will record
  its state if --save-state is passed


### v0.2.0.0

- [Consumer] Add the ability to save & restore stream state (i.e. last consumed
  sequence number per shard).

- [Consumer CLI] Add --save-state, --restore-state options

### v0.1.0.2

- Support specifying AWS keys as options to the CLI (either directly or from a
  file). You must specify one of these options.

- Remove filtering & date range capabilities from the CLI; remove `--raw` option,
  which will now be the only behavior.

- Fix CLI to print unescaped strings.

### v0.1.0.1

- Add some throttling to the consumer loop to avoid rate limiting
- Relax the lower-bound on `monad-control`
- Upgrade to `lens-4.7`, add `lens-action` dependency

### v0.1.0.0

Initial release
