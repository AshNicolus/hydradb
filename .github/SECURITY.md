# Security policy

## Reporting a vulnerability

Email **cloud@hydradb.com**. Do not open a public issue for a
vulnerability.

Include what you can: what you found, how to reproduce it, and what an attacker
gets. A proof of concept helps. You will get an acknowledgement within three
working days.

Do not test against a deployment you do not own. Run HydraDB locally instead
(see the README).

## What is not a vulnerability

- **The admin port has no authentication for `/livez`, `/readyz` and
  `/metrics`.** This is on purpose: health probes and metrics scrapers use
  them. The admin server binds to `0.0.0.0:9090` by default. Put the port
  behind a network policy or a firewall. The Helm chart includes a
  `NetworkPolicy`. You can also set `GRAPH_ADMIN_ADDR` to a loopback address.
- **`GRAPH_ALLOW_PLAINTEXT=true` turns off TLS.** This setting exists for local
  development. Do not use it in a deployment that other people can reach.

## Supported versions

Fixes land in the next release. There are no backported releases.
