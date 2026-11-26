#!/usr/bin/env python3
"""Activate idle image catalogs and their matching generic restore pool atomically.

Run after image-template/prepare.py, before starting/restarting the conductor.
This publishes configuration only; it never stops services or runs tenant code.
"""
import argparse
import base64
import hashlib
import json
import os
from pathlib import Path
import stat
import tempfile


def activate(config_path, catalog_paths, credential_path, pool_name):
    config = json.loads(config_path.read_text())
    credential = json.loads(credential_path.read_text())
    fingerprint = hashlib.sha256(credential['key'].encode()).hexdigest()
    images = set()
    resolved = []
    for path in catalog_paths:
        path = path.resolve(strict=True)
        catalog = json.loads(path.read_text())
        if catalog.get('application_started') is not False:
            raise ValueError(f'{path}: application must not have started')
        if catalog.get('language_specific_preparation') is not False:
            raise ValueError(f'{path}: preparation must be language independent')
        identity = catalog['identity']
        if identity['credential_fingerprint'] != fingerprint:
            raise ValueError(f'{path}: credential mismatch')
        if identity['image_template'] in images:
            raise ValueError('duplicate image catalog')
        images.add(identity['image_template'])
        if not catalog['template']['templateID'].startswith('e2b-snp-'):
            raise ValueError(f'{path}: expected an E2B snapshot')
        encoded = catalog['template']['templateID'][len('e2b-snp-'):]
        reference = base64.urlsafe_b64decode(encoded + '=' * (-len(encoded) % 4)).decode()
        if not reference.startswith('file://'):
            raise ValueError(f'{path}: generic preparation cache requires a local snapshot')
        restore = json.loads(catalog['template']['metadata']['kuasar-sandbox.restore'])
        if restore.get('fork_identity') is not True:
            raise ValueError(f'{path}: identity renewal is mandatory')
        if catalog.get('host_preflight_version') != 1:
            raise ValueError(f'{path}: restore preflight is incomplete')
        if Path(catalog['staging_credential_file']).resolve() != credential_path.resolve():
            raise ValueError(f'{path}: staging credential file mismatch')
        if catalog['cache']['errors'] or catalog['cache']['application_executed']:
            raise ValueError(f'{path}: preparation validation failed')
        resolved.append(str(path))
    if not resolved:
        raise ValueError('at least one prepared image catalog is required')
    pools = [p for p in config['units']['runner_pools'] if p.get('name') == pool_name]
    if len(pools) != 1 or not pools[0].get('prepared_vm') or pools[0]['size'] <= 0:
        raise ValueError('expected one nonempty generic prepared VM pool')
    # Never publish snapshot mappings without a matching restore-capable pool.
    pools[0]['prepared_restore'] = True
    config['image_template_catalogs'] = resolved
    mode = stat.S_IMODE(config_path.stat().st_mode)
    fd, temporary = tempfile.mkstemp(prefix='.catalog-config-', dir=config_path.parent)
    try:
        with os.fdopen(fd, 'w') as stream:
            os.fchmod(stream.fileno(), mode)
            json.dump(config, stream, indent=2)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, config_path)
        directory = os.open(config_path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)
    return {'catalogs': len(resolved), 'pool': pool_name, 'slots': pools[0]['size'],
            'strategy': 'image catalog -> shared generic restore pool'}


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', type=Path, required=True)
    parser.add_argument('--catalog', type=Path, action='append', required=True)
    parser.add_argument('--credential', type=Path, required=True)
    parser.add_argument('--pool-name', default='prepared')
    args = parser.parse_args()
    print(json.dumps(activate(args.config, args.catalog, args.credential, args.pool_name)))
