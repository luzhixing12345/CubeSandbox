# Commit a Running Sandbox as a Template

The `tpl commit` command creates a new template from the current filesystem and memory state of a running sandbox. The required `--sandbox-id` option identifies the source sandbox.

By default, the CLI sends only the sandbox ID. CubeMaster restores the complete create-time request from the stored sandbox spec. For legacy sandboxes without a stored spec, CubeMaster falls back to the create request saved in the sandbox's origin template.

The CLI displays build progress and waits until template creation succeeds or fails.

## Commit the Sandbox

```bash
cubemastercli tpl commit \
  --sandbox-id <sandbox-id>
```

After a successful commit, CubeMaster generates a new `tpl-...` template ID and creates its initial replica on the source sandbox's node.

## Override the Create Request

Use `--file <path>` to supply a complete `CreateCubeSandboxReq` that overrides the automatically restored request. The file replaces the restored request; the two are not merged.

When using `--file`, the following options can modify `cube_network_config` in the file request:

| Option | Effect |
| --- | --- |
| `--allow-internet-access=false` | Explicitly set the request's internet access value. |
| `--allow-out-cidr <cidr>` | Append an allowed egress CIDR; repeat for multiple CIDRs. |
| `--deny-out-cidr <cidr>` | Append a denied egress CIDR; repeat for multiple CIDRs. |

Network override options require `--file`. For example:

```bash
cubemastercli tpl commit \
  --sandbox-id <sandbox-id> \
  --file template-request.json \
  --allow-internet-access=false
```
