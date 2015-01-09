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
