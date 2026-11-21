# Idle image snapshot templates

This opt-in path prepares an idle E2B image once, then selects its reusable
snapshot for ordinary image-ID create requests. It does not run the image's
interpreter or application during preparation, and does not maintain a separate
running VM pool per image.

Build the guest-runtime bundle with its `image-metadata` helper first. Run
`prepare.py` with the native console credential file, immutable image registry
record, proxy URL, and a private output catalog path. `--read-budget 0` prepares
only image-tree metadata. A nonzero budget (maximum 512 MiB) reads regular files
uniformly, without following symlinks or crossing filesystems. This is an
explicit preparation cost, not part of measured warm create latency.

Add the resulting absolute catalog paths to the conductor's
`image_template_catalogs` array. Catalogs are scoped to the exact image ID and
credential fingerprint. Requests with custom metadata, environment, workspaces,
MMDS headers or auto-reset keep their existing cold-create behavior. Direct
snapshot requests and running-parent forks remain separate operations. The
returned sandbox records its actual snapshot template as its template ID.

For the private-RAM local template fast path, set
`KUASAR_TEMPLATE_MEMORY_CACHE=1` **only on the intended runner service**. The
sandboxer's restore code then prepares a sparse, read-only RAM representation
in private tmpfs. Each instance pins its inode and Cloud Hypervisor maps it
`MAP_PRIVATE`; writes do not modify the template. The cache requires a standalone immutable
snapshot reference and is namespaced by the customer-key fingerprint. Durable
Snapshot S, its disk graph and normal authentication remain authoritative; the
cache is not a replacement snapshot format or persistence destination.

Cache preparation is one-time work and must be reported separately. It is
triggered by the first restore; `prepare.py` now performs and reports this
priming restore before publishing a catalog. Do not discard this cost as a warmup. Cache contents can be regenerated from the durable snapshot after
host restart. The current total allocated-data budget is 1 GiB. Active VM pins
prevent eviction. If there is insufficient budget, restore retains the existing
on-demand paging path. Disk writes and checkpoints continue through the native
persistent CoW owner and its synchronization barriers.

## Generic prepared restore slots

Configure a runner pool with `prepared_vm: true` and `prepared_restore: true`.
This pool starts an empty CH process and calls `vmm.prepare` for its configured
CPU profile. No guest is booted and no image, snapshot, guest RAM or tenant
credential is installed in an idle slot. The same reserve accepts different
image snapshot templates. Keep an ordinary runner pool for cold/custom creates
and incompatible requests. A booted late-bind image pool uses `prepared_vm`
without `prepared_restore`; those slots are never confused with restore slots.

On claim, the normal durable assignment transfers the preallocated network
lease. Validated snapshot and disk state are prepared normally, then the
existing CH process receives `vm.restore` in CopyOnWrite mode. The existing KVM
and vCPU handles are retained. Network identity renewal, guest acknowledgement,
route readiness and persistence barriers remain in the normal restore path.
The runner owns and reaps an unused child; successful transfer hands the one
child reaper to ServeAndWait. Cancellation or restore failure closes the holder.
If the bounded RAM cache cannot admit a snapshot, its empty holder is released
before the ordinary on-demand restore path is used. This fallback is reported
in runner logs, not counted as KVM reuse.

Custom create options currently use the cold image path rather than modifying
a captured guest state. Running-parent fork remains a distinct operation with
its existing process/memory inheritance semantics.

Measure API request to the client's shell Ready marker and fresh interpreter
print separately. Use the same digest, command and cache preparation policy for
both products; report image preparation, first cache fill and idle-slot/refill
costs separately. Confirm reuse using the same CH PID and a trace showing no
KVM_CREATE_VM or KVM_CREATE_VCPU during claim, not merely the configured flag.
