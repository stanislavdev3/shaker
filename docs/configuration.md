# TOML configuration

The application reads one TOML file during startup. The path is selected with
`--config` and defaults to `config.toml`. Environment variables are not read, there is
no override layer, and configuration is not reloaded while the process is running.
Restart the affected service after an atomic file replacement.

The decoder rejects unknown keys, invalid types, non-positive durations, and files
larger than 1 MiB. Durations use Go notation such as `30s`, `5m`, and `24h`.
`config.example.toml` documents every setting and contains development-only placeholder
secrets.

## Commands

```text
earthquake-service --config /etc/shaker/api.toml api
earthquake-service --config /etc/shaker/core.toml core
earthquake-service --config /etc/shaker/notification.toml notification
earthquake-service --config /etc/shaker/provider-emsc.toml provider-worker emsc
```

The provider name is a deployment identity rather than a mutable setting. The selected
`providers.<name>` table supplies its endpoint, intervals, limits, and `state_file`.

## Service-scoped files

Production deployments should not mount the complete development example into every
container. Create a separate file for each service and omit credentials that role does
not use:

- provider-worker needs `[app]`, `[kafka]`, and its selected `[providers.<name>]` table;
- core needs `[app]`, `[database]`, `[kafka]`, and `[ingestion]`;
- notification needs `[app]`, `[database]`, `[kafka]`, `[notification]`, and `[security]`;
- API needs `[app]`, `[database]`, `[api]`, `[security]`, and optionally
  `[administration]` and `[observability]`.

Omitted optional values receive the documented defaults. Role-required credentials and
addresses are validated after decoding. Core and provider-worker do not require the
encryption key; provider-worker does not require a database URL.

Configuration files can contain database passwords, API keys, encryption keys, and bot
tokens. Keep production files outside the repository, restrict them to the service
account, mount them read-only, exclude them from image layers and backups where
appropriate, and rotate them through the deployment secret-management process.
