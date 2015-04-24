### v0.4.0.0

This release contains breaking changes.

- Replaced the internal use of `MonadError` with proper open-world exceptions.
  The exposed operations which may throw exceptions have been adjusted to
  return `m (Either e a)`; resolves [issue
  \#17](https://github.com/alephcloud/hs-aws-kinesis-client/issues/17).

- [Producer] Implemented a cleaner shutdown routine policy: previously, the
  producer would stay alive indefinitely until the thread on which it was
  created is killed. Now, the producer is shut down immediately once the scope of
  `withKinesisProducer` exits, at which point the queue will be flushed according
  to a configurable (and optional) timeout. Resolves [issue
  \#19](https://github.com/alephcloud/hs-aws-kinesis-client/issues/19).

- [Producer] Restructured the monolithic error sum types into many smaller
  types to better support the standard mode of use for error handling.

- [Consumer] Removed unused `ConsumerError` type.

- [Consumer] Changed batch size to use `Natural` type rather than `Int`.

- [Producer] Parameterized the implementation by an arbitrary queue structure;
  by default, the producer's message queue is now implemented using `TBMChan`
  to avoid a bug in `TQueue` which causes live-locking under very heavy load.

Other non-breaking changes:

- [Producer] A better, more reliable implementation of consuming the input
  queue in chunks.

- Removed dependencies on `either`, `errors`, `hoist-error`, `stm-conduit`;
  added dependency on `enclosed-exceptions`.

- Increased lower bound on `monad-control` to 1.0 (for hygienic purposes).

### v0.3.0.2

- Upgrade aws-general, aws-kinesis lower bounds.

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
