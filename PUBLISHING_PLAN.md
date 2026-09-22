# Path to a crates.io release

Prompted by writing up this project for the blog and realizing the README's
`cargo install laplink-p2p` doesn't actually work yet -- the crate isn't published.
This tracks what's blocking a real `0.31.0` (or `1.0.0`) release to crates.io.

## Already fixed (this pass)

- **CI has been red on every platform for a while, and it's one root cause.**
  `RUSTFLAGS: -Dwarnings` turns every warning into a hard build error, and the
  unused `qr_to_braille` function in `src/qr.rs` (dead code since it was written
  and never wired into a binary) has been tripping that on every single matrix
  cell -- Linux, macOS, Windows, MSVC and GNU, stable/beta/nightly, `lint` and
  `check-deps` too. See run [35647088111](https://github.com/jfernand/laplink-p2p/actions/runs/35647088111):
  every job fails on the identical `error: function \`qr_to_braille\` is never used`.
  This was never actually a cross-platform bug; it was one `#[allow(dead_code)]`
  away from a fully green matrix. Fixed by annotating the function and explaining
  in a doc comment why it's there and unused (see ROADMAP.md item on QR codes).
- `Cargo.toml` had no `repository` or `homepage`, which `cargo publish` warns
  about and which matters more than usual here, since there's no other way for
  someone to go audit the source of a tool that's willing to overwrite its own
  binary. Added both, pointing at the GitHub repo.
- `Cargo.lock` pinned yanked versions of `crossbeam-channel` (0.5.14) and `spin`
  (0.9.8), which `cargo publish --dry-run` flags. Ran `cargo update` on both;
  moved to 0.5.17 and 0.9.9 respectively, both non-yanked.
- `cargo publish --dry-run` is now warning-free.

**Test suite after the bump**: `cargo test --release` gives 7 passed, 3 failed
(`send_recv_file`, `send_recv_dir`, `ll_remembers_ticket`), all three failing
identically with `error: timed out` while `ll receive` tries to hole-punch to a
separate `ll send` subprocess. Verified this is pre-existing and unrelated to
the dependency bump by stashing these changes and re-running `send_recv_file`
against the untouched `9196d59` baseline -- same failure, same error. Reads
like the sandbox this was run in doesn't have real outbound P2P connectivity,
not a code regression. The three that failed are exactly the ones that need a
real network path between two processes; everything that stays on localhost or
within one process (filesystem watching, the subscription stream, self-update
detection and apply, ticket persistence) passed clean. Worth re-running the
full suite somewhere with real network access before trusting it fully, but
nothing here points at the dependency bump as the cause.

**Not yet pushed to `origin/main`** -- these are local commits on top of
`9196d59` (`origin/main` is at `a7bad6d`). Push once confirmed on a machine
with real network access, and watch the next CI run actually go green for the
first time in a while.

## Still open, needs a decision (not something to silently fix)

### 1. Self-update should probably be opt-in, not default-on

`ll-tui` will, by default, notice a newer release sitting in whatever folder
it's browsing and offer to download it and overwrite its own running binary
(`self_replace`). That's a reasonable feature for two people on machines they
control. It's a different risk profile once this is a public crate and the
binary doing the self-replacing was `cargo install`ed by someone who has no
relationship with the folder they're browsing.

Options, roughly in order of how much I'd trust them for a first public release:
- Require an explicit `--allow-self-update` flag (or a config file opt-in) before
  the update banner/`u` keybinding even activates. Off by default.
- Keep it default-on, but add a second confirmation step (`u` shows a
  "download and replace this binary? [y/N]" instead of doing it on one keypress).
- Ship it as-is and document the behavior loudly in the README's own bold
  warning up top. Weakest option; I'd rather not.

I'd lean toward the first option for `0.31.0`, and revisit making it default-on
once the tool has some track record.

### 2. Binary names are collision-prone

`ll`, `ll-serve`, `ll-tui` are short, generic names that `cargo install` drops
straight into `~/.cargo/bin`. `ll` in particular is exactly the kind of name
someone else's crate could also claim, or that collides with a personal shell
alias. Two ways to go:
- Rename the installed binaries (e.g. `llink`, `llink-serve`, `llink-tui`) --
  a real breaking change for the two of us already using this, but painless
  since there's no public install base yet.
- Keep the names, accept the collision risk, document it.

Given nobody but us has `cargo install`ed this yet, renaming now is nearly free
and gets more expensive the longer it waits. I'd rename before publishing.

### 3. macOS is missing from the actual release artifacts

`.github/workflows/release.yml`'s build matrix has macOS commented out
(`runner: [ macOS, ARM64]`, `target: darwin-x86_64` -- note the target/runner
mismatch too, x86_64 target on what reads like an ARM64 label). The `tests`
workflow *does* run the test suite on `macOS-latest` (GitHub-hosted), so the
code itself gets exercised there; what's missing is a shippable macOS release
binary, which also means the self-updater's `darwin-*` asset matching in
`update.rs` has nothing to ever find on macOS.

This is a runner-availability/hosting decision I don't want to make
unilaterally -- was the self-hosted macOS runner intentionally pulled (cost,
availability, flakiness), or just left half-wired? Worth resolving before
claiming macOS support in the README, one way or the other: either get a
working macOS release leg (self-hosted or GitHub-hosted `macos-latest`,
matching the actual target it builds), or drop macOS from the supported-platform
claims until there is one.

### 4. First real release should be a version bump with a changelog

Once 1-3 are settled, cut `0.31.0` (or jump to `1.0.0` if the intent is to
signal API stability -- worth a separate call), tag it, let `release.yml`
build artifacts for whatever platforms are actually supported by then, and
*then* `cargo publish`. Not blocking, just sequencing: don't publish `0.30.3`
as-is with a stale version number once the above changes land.
