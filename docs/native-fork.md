
### Generic-slot restore materials

Registered image catalogs can prepare restore materials in each generic slot,
without creating a VM pool per image. A slot still owns one pre-created KVM VM
and its vCPUs; multiple compatible catalogs retain separate metadata and unused
persistent CoW writers. No tenant interpreter is started during preparation.

The catalog publisher records `staging_credential_file`, referencing the
operator's private console configuration. The conductor verifies that its token
matches the catalog fingerprint and remains authorized before passing the
manifest key to the exact slot runner over its authenticated configuration
channel. A sealed, close-on-exec descriptor carries the preparation capsule to
the privileged VMM holder; it is not passed into Cloud Hypervisor or the guest.
The catalog and credential file must remain operator-controlled.

Before publishing a slot as ready, the holder checks source versions and the
CPU/memory shape, resolves the runtime, validates the disk graph, prepares the
configuration rewrite, creates persistent disk branches in their final slot
location, and flushes their contents and directories. Claim still performs
normal durable assignment and admission. It requires the same authorized key,
source, storage configuration, source-file versions and disk graph; a prepared
plan is consumed only once. An unmatched or unsupported source uses the normal
restore path. Unsupported catalogs do not prevent a generic slot from becoming
ready.

Claim transfers never-written CoW backends, including their open writer FDs,
into the admitted sandbox's accounted cache. It does not reopen the writers or
repeat their recovery scan. The current disk-graph retention barrier still joins
before the VM resumes. Unselected plans close their backends before their source
readers, then remove only their unchanged, unused disk files. Cancellation and
failed handoff close all staged resources. Preparation costs and the number of
registered catalogs must be reported separately from request latency.

This optimization applies to catalog restore. Forking a running parent keeps
its existing live-state capture, safety, persistence and network-isolation
semantics; it does not substitute a template for the parent's current state.

### Old-fork preparation/claim alignment

The generic restore path now follows the old pool's ordering at the following
boundaries (without adopting its older control-plane or guest ABI):

| Boundary | Current implementation |
| --- | --- |
| KVM/vCPU preparation | One empty prepared VM per generic slot; independent of selected image. |
| Artifact handoff | Authenticated helper retains exact preparation results in the slot runner; claim rechecks key-bound request identity, dependency versions and the accepted S/E pair, then binds the resolution digest to the assigned run. |
| Disk branch | Persistent empty CoW writers are flushed before pool readiness and remain open; claim transfers them to the admitted cache. |
| CH restore metadata | Per-catalog config/state files are prepared on the runtime filesystem. Claim verifies the final identity and exact bytes and publishes the prepared files; no per-image VM is created. |
| Network | Slot owns a provider port/TAP; claim still revalidates the lease and keeps the network gate closed through guest identity renewal. |
| Resume/connection | After disk retention joins, the complete vm.resume request is submitted before starting the guest handshake. The response and handshake overlap; both must succeed before readiness. Failures cancel and join both operations and close the connection. |
| Live parent fork | Captures the parent's live state and retains the complete identity-cleanup path; catalog materials cannot replace the running parent's state. |

The old guest's empty-TCP certificate cannot be asserted for an E2B template
that already has envd listening on TCP. Such templates keep the socket checks;
removing them would change the isolation contract. Likewise, the new guest's
inner-executor/MUX readiness and envd initialization are required by its own ABI.
These are explicit architecture differences, not evidence that measured latency
is equivalent to the old fork. End-to-end performance must still be reported.

### Restore initialization and transport reuse

With `sandbox.restore_envd_init: true`, E2B memory restores without MMDS carry
initialization through the existing restore handshake. The inner executor calls
its own envd `/init` after guest identity renewal and thaw. The guest only returns
`restore_envd_initialized` after HTTP 204; the host still joins persistence,
resume, initialization and network barriers before Ready. Environment, default
user/workdir and timestamp retain their original semantics. No interpreter or
user command runs in this preparation. The flag defaults to false and requires
a compatible sandbox-ctl; MMDS rekeying and live-parent fork retain their existing
paths. Older snapshots that do not acknowledge the extension use the original
private-UDS init before publishing Ready. Failed initialization never advertises
readiness. Updating guest code requires new snapshots, not just a new host binary.

A consumed raw forwarding transport can now replenish one bounded spare. The
spare contains no guest request or application bytes; expiration, cancellation,
pause/drain and close retain ownership. Generation checks reject late completion
from an earlier pause cycle. This is connection preparation, not language warming.

`fork_restore` also takes the same lifecycle mutex as ordinary restore and
quiesce, so identity renewal cannot race another lifecycle transition.
