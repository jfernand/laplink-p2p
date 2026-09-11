# laplink-p2p

This is an example application using [iroh](https://crates.io/crates/iroh) with
the [iroh-blobs](https://crates.io/crates/iroh-blobs) protocol to send files and
directories over the internet, and to browse/download files from a small
peer-to-peer file server.

It is also useful as a standalone tool for quick copy jobs.

Iroh will take care of hole punching and NAT traversal whenever possible,
and fall back to a relay if hole punching does not succeed.

Iroh-blobs will take care of [blake3](https://crates.io/crates/blake3) verified
streaming, including resuming interrupted downloads.

laplink-p2p works with 256 bit node ids and is, therefore, location transparent. A
ticket will remain valid if the IP address changes. Connections are encrypted
using TLS.

# Installation

```
cargo install laplink-p2p
```

This installs three binaries: `ll`, `ll-serve`, and `ll-tui`.

# Usage

## `ll`: one-shot send/receive

### Send side

```
ll send <file or directory>
```

This will create a temporary [iroh](https://crates.io/crates/iroh) node that
serves the content in the given file or directory. It will output a ticket that
can be used to get the data.

The provider will run until it is terminated using `Control-C`. On termination, it
will delete the temporary directory.

This currently will create a temporary directory in the current directory. In
the future this won't be needed anymore.

### Receive side

```
ll receive <ticket>
```

This will download the data and create a file or directory named like the source
in the **current directory**.

It will create a temporary directory in the current directory, download the data
(single file or directory), and only then move these files to the target
directory.

On completion, it will delete the temp directory.

All temp directories start with `.ll-`.

## `ll-serve` / `ll-tui`: browse a folder over the network

`ll-serve <folder>` starts a long-running node that publishes a listing of
`<folder>` and prints a ticket:

```
ll-serve <folder>
```

`ll-tui <ticket>` connects to it, shows the folder structure, and lets you
download individual files to the current directory:

```
ll-tui <ticket>
```

`ll-serve` keeps a persistent blob store (`.ll-serve-store` inside the served
folder by default) so it can be restarted against the same folder without
re-importing everything from scratch.
