# Dataset findings triage

Use this after the Semgrep phase completes.

The point is not to repeat Semgrep output. The point is to decide whether each
finding is a real dataset security issue.

## Dataset trust model

A dataset's `load()` method processes data the user did not author. This is the
**data trust boundary**: the loaded file may be malicious.

Any deserialization that can execute arbitrary code (pickle, torch, joblib,
dill, cloudpickle) must satisfy all three of these conditions:

1. **Documented as unsafe** — the docstring states that loading can execute
   arbitrary code and that the data source must be trusted.
2. **Defaults to the safest available option** — e.g. `weights_only=True` for
   `torch.load`, not the other way around.
3. **Never advertised as safer than it is** — if the docstring claims to "avoid
   pickle" or be "secure", the implementation must actually deliver that.

If any condition is violated, the dataset has a security defect regardless of
whether the underlying library behaviour is well-known.

## Classification buckets

### `dataset_vulnerability`

Use when the dataset code itself is unsafe, misleading, or silently drops
safety arguments. Examples:

- A dataset calls `torch.load` without `weights_only=True` and has no opt-out
- A dataset builds `self._load_args` from user config but passes a different
  dict to the underlying library call (silent arg drop)
- A docstring claims the dataset avoids pickle or is "secure" when it is not
- A dataset deserialises untrusted data with no safety default and no
  documentation of the risk

Severity determines how the finding is surfaced:

- **ERROR** — high confidence, critical impact (RCE-capable load with no
  opt-out, broken safety args, misleading safety claims). **Flag prominently
  and recommend fixing before merge.**
- **WARNING** — plausible risk but lower confidence or limited impact (missing
  risk documentation, no default safeguard but user can opt in). **Recommend
  investigating; does not need to block the merge.**

### `by_design_with_documentation`

Use when the risk is inherent to the dataset's purpose or arises from how the
user configured it, **and** the risk is documented. Examples:

- `PickleDataset` uses pickle — that is its entire purpose
- A user sets `backend: dill` in catalog.yml for `PickleDataset`
- A user sets `file_format: pickle` in `GenericDataset`
- `load_dataset` from Hugging Face runs Hub scripts — documented and opt-in

**Suggested next step:** not actionable in dataset library code. Count in the
summary but do not itemise in the findings section.

### `needs_manual_review`

Use when deeper context is required before classifying confidently.

**Suggested next step:** open a follow-up issue or assign a human reviewer;
do not auto-dismiss.

## Manual review checks

Run these after Semgrep, regardless of whether any rule fired. Semgrep only
covers known patterns. These checks catch the class of issues that static
analysis misses.

Apply the same path exclusions as the Semgrep scan when searching
(`**/tests/**`, `**/docs/**`, `**/features/**`, `.agents/`, `tools/`,
`**/kedro_datasets_benchmarks/**`).

### 1. load_args / save_args passthrough

For every dataset that accepts `load_args` or `save_args` in `__init__`:

- Trace whether `self._load_args` is actually passed to the underlying library
  call in `load()` (and likewise `self._save_args` in `save()`).
- If the dataset builds `_load_args` but passes a different dict (e.g.
  `_fs_open_args_load`) to the library, this is a **silent arg drop** — the
  user cannot control safety-relevant parameters. Classify as
  `dataset_vulnerability` at ERROR severity.

This check exists because of a real bug: `PyTorchDataset` built `_load_args`
with `weights_only=True` but passed `_fs_open_args_load` to `torch.load`
instead, making the safety default ineffective.

### 2. Docstring accuracy for safety claims

For any dataset whose docstring contains words like "safe", "secure", "avoid
pickle", "recommended serialisation protocol", or similar safety claims:

- Verify the implementation actually delivers on the claim.
- If the dataset uses `pickle.load`, `torch.load`, `joblib.load`, or any other
  code-execution-capable deserializer, the safety claim is misleading.
  Classify as `dataset_vulnerability` at ERROR severity.

### 3. Risk documentation for binary deserializers

For any new dataset that uses a binary deserialization method (`torch.load`,
`joblib.load`, `tf.keras.models.load_model`, Darts model loading, or similar):

- Check whether the docstring warns that loading untrusted files can execute
  arbitrary code.
- If there is no warning, classify as `dataset_vulnerability` at WARNING
  severity.

### 4. Security suppression comments

Search scanned files for comments that suppress security scanner findings:
- `# nosec` (Bandit)
- `#nosec` (Bandit, no space variant)
- `# nosemgrep` (Semgrep)

A suppression comment makes the flagged code invisible to the scanner. If the
suppression is unjustified or outdated, it silently masks a real vulnerability.

For every match, classify as `needs_manual_review` and ask the reviewer to
verify:
1. The suppression is still necessary — the underlying issue has not been
   refactored away.
2. The risk is documented — there is an adjacent comment explaining **why** the
   suppression is safe.
3. There is no code-level fix that would remove the need for suppression
   entirely.

**PR mode — new vs. pre-existing suppressions:**

In PR mode, compare each hit against the PR diff (`gh pr diff <number>`).
- A suppression whose line appears in the diff (added or modified) is a **new
  suppression** — flag it with higher priority.
- A suppression on a line not in the diff is **pre-existing** — still flag it
  as `needs_manual_review`, but note that it predates this PR.

In full-codebase mode, all suppressions are treated equally.

## General ruleset guidance

### `p/security-audit`

_Broad OWASP-style checks — injection, unsafe calls, dangerous APIs._

Expect high volume; most findings on a dataset library are
`by_design_with_documentation`. Apply standard classification buckets.

### `p/secrets`

_Detects hardcoded credentials, tokens, and API keys in source files._

Triage in order: (1) check for a placeholder value (`"YOUR_API_KEY"`, `"xxx"`,
`"<token>"`) — classify as `by_design_with_documentation`; (2) if the string
looks like a real secret, classify as `needs_manual_review`.

### `p/python`

_General Python anti-patterns — unsafe deserialization, shell injection,
insecure stdlib use._

Key exception: `pickle` usage — if a dataset deserialises data via pickle and
this is the dataset's documented purpose (e.g. `PickleDataset`), classify as
`by_design_with_documentation`. If a dataset uses pickle but does not document
the risk, classify as `dataset_vulnerability`.

## Dataset-specific rule guidance

### `datasets-torch-load-no-weights-only`

Fires on `torch.load(...)` without `weights_only=True`. Without this flag,
`torch.load` deserialises arbitrary pickle data.

- If the dataset provides `weights_only=True` as a default in `load_args` and
  passes `self._load_args` to `torch.load`, verify the args are actually
  reaching `torch.load` (see manual check #1). If they are, classify as
  `by_design_with_documentation` — the user can override but the default is
  safe.
- If `weights_only` is not set or the load_args are silently dropped, classify
  as `dataset_vulnerability` at ERROR severity.

### `datasets-joblib-load`

Fires on any `joblib.load(...)` call. Joblib uses pickle internally.

- If the dataset documents the risk and the data source is trusted by design
  (e.g. MLRun artifacts from a controlled registry), classify as
  `by_design_with_documentation`.
- If there is no risk documentation, classify as `dataset_vulnerability` at
  WARNING severity.

### `datasets-dynamic-read-dispatch`

Fires on `getattr(module, f"read_{format}")` and similar patterns. If
`format` is set to `"pickle"`, this dispatches to `read_pickle`.

- If the dataset validates or restricts the allowed format values, classify as
  `by_design_with_documentation`.
- If any string is accepted without validation, classify as
  `dataset_vulnerability` at WARNING severity — recommend documenting that
  `pickle` format enables code execution, or restricting the allowed values.

### `datasets-hub-remote-code`

Fires on `load_dataset(...)` without `trust_remote_code=False`.

- If the dataset passes through user-controlled kwargs that can include
  `trust_remote_code`, and this is documented, classify as
  `by_design_with_documentation`.
- If the call always trusts remote code with no user opt-out, classify as
  `dataset_vulnerability` at WARNING severity.

### `kedro-dynamic-import`

Fires on `importlib.import_module($X)` with a non-literal argument.

- If the module path comes from catalog config (e.g. `backend` parameter in
  `PickleDataset`), classify as `by_design_with_documentation` — the user
  controls what backends they install and configure.
- If the module path could be influenced by untrusted external input with no
  validation, classify as `dataset_vulnerability`.

### `kedro-yaml-unsafe-load`

Fires on `yaml.load()` without a safe Loader.

- If found in dataset code, classify as `dataset_vulnerability` — dataset code
  should always use `yaml.safe_load()`.
- `yaml.safe_load()` is a separate function and is never matched by this rule.

### `kedro-subprocess-shell-injection`

Fires on `subprocess.$FUNC($CMD, ..., shell=True)` with a non-literal command.

- If `$CMD` can be influenced by catalog config or user input, classify as
  `dataset_vulnerability` at ERROR severity.
- If the command is hardcoded from framework-internal defaults, classify as
  `by_design_with_documentation`.
