#!/usr/bin/env python3
"""Create a private E2B SDK overlay; never modify a shared SDK installation.

Serializes concurrent API transport initialization and shares lazily initialized
default TLS trust contexts. TLS verification, credentials, per-thread Envd
connection pools, and API timeouts remain unchanged. Explicit SSL_CERT_FILE/DIR
settings retain the original HTTPX context policy. Run with the target SDK Python.
The target file hash pins the supported SDK input; unknown versions fail closed.
"""
import argparse
import hashlib
from pathlib import Path
import shutil
import subprocess

EXPECTED = '738df96013c9e9f1a5606807baa49a2d799ff5b67d52f91f7184f854f904dcff'

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', type=Path, required=True, help='SDK root containing e2b/')
    parser.add_argument('--destination', type=Path, required=True, help='new private overlay directory')
    args = parser.parse_args()
    from importlib.metadata import version
    if version("httpx") != "0.28.1":
        parser.error("HTTPX version mismatch: review SSL context semantics before applying")
    source = args.source.resolve()
    destination = args.destination.resolve()
    target = source / 'e2b/api/client_sync/__init__.py'
    if hashlib.sha256(target.read_bytes()).hexdigest() != EXPECTED:
        parser.error('SDK version mismatch: review the patch for this version first')
    if destination.exists():
        parser.error('destination already exists; choose a new directory')
    patch = Path(__file__).with_name('e2b-transport-singleflight.patch')
    if not patch.is_file():
        parser.error('missing companion patch')
    if shutil.which('git') is None:
        parser.error('git is required to apply the reviewed patch')
    destination.mkdir(parents=True)
    shutil.copytree(source / 'e2b', destination / 'e2b', ignore=shutil.ignore_patterns('__pycache__', '*.pyc'))
    subprocess.run(['git', 'apply', '--unidiff-zero', str(patch.resolve())], cwd=destination, check=True)
    print(f'Private SDK installed at {destination}. Put this directory first in PYTHONPATH.')

if __name__ == '__main__':
    main()
