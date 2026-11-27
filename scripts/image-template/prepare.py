#!/usr/bin/env python3
"""Prepare a reusable, idle image snapshot; never start a tenant interpreter.

The result is a normal native snapshot template and can be used by the E2B SDK.
This prepares files, not a per-image pool of running VMs. Restore-slot reuse is
an independent capability; callers must not label a cold restore as pool restore.
"""
import argparse
import fcntl
import hashlib
import json
import os
from pathlib import Path
import tempfile
import time
import urllib.request

from e2b import Sandbox

# Bump when authenticated host preflight receipts change format/coverage.
HOST_PREFLIGHT_VERSION = 1


def durable_json(path, value):
    fd, temporary = tempfile.mkstemp(prefix='.template-', dir=path.parent)
    try:
        with os.fdopen(fd, 'w') as stream:
            json.dump(value, stream, indent=2)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def prime_restore(args, console, result):
    """Pay and report the first restore/cache-fill cost before publishing."""
    start = time.monotonic_ns()
    template = result['template']
    child = None
    try:
        child = Sandbox.create(template['templateID'], metadata=template.get('metadata', {}),
            timeout=600, api_key=console['key'], api_url=console['api'], sandbox_url=args.proxy)
        check = child.commands.run('/opt/sandbox-runtime/bin/image-metadata / 0',
            user='root', cwd='/', timeout=60)
        if check.exit_code or json.loads(check.stdout)['errors']:
            raise RuntimeError('template priming restore failed validation')
    finally:
        if child is not None:
            child.kill()
    result['host_preflight_version'] = HOST_PREFLIGHT_VERSION
    result['costs_ms']['first_restore_priming_including_delete'] = (time.monotonic_ns()-start)/1e6
    result['costs_ms']['total_preparation'] = (
        result['costs_ms']['total_including_source_delete'] +
        result['costs_ms']['first_restore_priming_including_delete'])


def pause_and_publish(console, sandbox_id):
    base = console['api'].rstrip('/') + '/sandboxes/' + sandbox_id
    headers = {'X-API-Key': console['key'], 'Content-Type': 'application/json'}
    for endpoint, body in [('/pause', {'memory': True}),
                           ('/export', {'toTemplate': True, 'keepSource': True})]:
        request = urllib.request.Request(base + endpoint,
            data=json.dumps(body).encode(), headers=headers)
        with urllib.request.urlopen(request, timeout=300) as response:
            raw = response.read()
    published = json.loads(raw)
    template = published.get('result')
    if not isinstance(template, str) or not template:
        raise RuntimeError('paused template publication returned no template ID')
    return {'templateID': template, 'metadata': {
        'kuasar-sandbox.restore': json.dumps({'fork_identity': True})}}


def prepare(args):
    if not 0 <= args.read_budget <= 512 * 1024 * 1024:
        raise ValueError('read budget must be between 0 and 512 MiB')
    console = json.loads(args.console.read_text())
    image = json.loads(args.image.read_text())
    identity = dict(image_template=image['template_id'], image_digest=image['manifest'],
                    read_budget_bytes=args.read_budget, policy_version=1,
                    api=console['api'], credential_fingerprint=hashlib.sha256(console['key'].encode()).hexdigest())
    args.output.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    # The caller owns this directory; the lock covers preparation and publication.
    with open(str(args.output) + '.lock', 'a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        if args.output.exists():
            old = json.loads(args.output.read_text())
            if old['identity'] != identity:
                raise ValueError('existing template has different image or preparation policy')
            if ('first_restore_priming_including_delete' not in old['costs_ms'] or
                    old.get('host_preflight_version') != HOST_PREFLIGHT_VERSION):
                prime_restore(args, console, old)
                durable_json(args.output, old)
            old["staging_credential_file"] = str(args.console.resolve())
            durable_json(args.output, old)
            return old
        pending = args.output.with_suffix(args.output.suffix + '.pending')
        if pending.exists():
            result = json.loads(pending.read_text())
            if result['identity'] != identity:
                raise ValueError('pending template has different preparation policy')
            result["staging_credential_file"] = str(args.console.resolve())
            prime_restore(args, console, result)
            durable_json(args.output, result)
            pending.unlink()
            return result
        start = time.monotonic_ns()
        sb = None
        try:
            sb = Sandbox.create(image['template_id'], timeout=600,
                api_key=console['key'], api_url=console['api'], sandbox_url=args.proxy,
                metadata={'kuasar-sandbox.checkpoint': '{"drop_caches":false}'})
            created = time.monotonic_ns()
            prepared = sb.commands.run(
                '/opt/sandbox-runtime/bin/image-metadata / ' + str(args.read_budget),
                user='root', cwd='/', timeout=60)
            if prepared.exit_code:
                raise RuntimeError('uniform image preparation failed: ' + prepared.stderr)
            cache = json.loads(prepared.stdout)
            if cache['errors'] or cache['application_executed']:
                raise RuntimeError('image preparation did not satisfy its policy')
            cached = time.monotonic_ns()
            # Template seeds are disposable: persist them through ordinary pause,
            # then publish the paused state. No running-snapshot API is required.
            snapshot = pause_and_publish(console, sb.sandbox_id)
            saved = time.monotonic_ns()
            result = dict(identity=identity, template=snapshot, cache=cache,
                staging_credential_file=str(args.console.resolve()),
                costs_ms={'create_to_api': (created-start)/1e6,
                          'wait_ready_and_prepare_cache': (cached-created)/1e6,
                          'snapshot_persistence': (saved-cached)/1e6,
                          'total_before_cleanup': (saved-start)/1e6},
                application_started=False, language_specific_preparation=False,
                restore_execution="selected_by_host_pool_configuration")
        finally:
            if sb is not None:
                sb.kill()
        result['costs_ms']['total_including_source_delete'] = (time.monotonic_ns()-start)/1e6
        # Preserve the durable source ref if priming fails; retry can resume it.
        durable_json(pending, result)
        prime_restore(args, console, result)
        # Advertise only after source cleanup, priming and durable save succeed.
        durable_json(args.output, result)
        pending.unlink()
        return result


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--console', type=Path, required=True)
    parser.add_argument('--image', type=Path, required=True)
    parser.add_argument('--proxy', required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--read-budget', type=int, default=0)
    opts = parser.parse_args()
    prepare(opts)
    print(opts.output)
