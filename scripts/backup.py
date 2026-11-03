#!/usr/bin/env python3
"""Linux/Windows Docker Compose backup/restore. Requires Python 3 and Docker only."""
import argparse
import datetime
import hashlib
import json
from pathlib import Path, PurePosixPath
import subprocess
import sys
import tarfile
import uuid

NAMES = ('database.dump', 'images.tar.gz', 'environment.env', 'compose.yml')

def run(args, **kw):
    return subprocess.run(args, check=True, **kw)

def output(args):
    return subprocess.check_output(args, text=True).strip()

def digest(path):
    h = hashlib.sha256()
    with path.open('rb') as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b''):
            h.update(chunk)
    return h.hexdigest()

def database(pg, program):
    # Credentials stay in the container environment, never in host command arguments.
    return ['docker', 'exec', pg, 'sh', '-ec', 'export PGPASSWORD="$POSTGRES_PASSWORD"; '+program]

def verify(source):
    manifest = json.loads((source / 'manifest.json').read_text(encoding='utf-8-sig'))
    entries = manifest.get('files', [])
    if manifest.get('version') != 1 or len(entries) != len(NAMES):
        raise ValueError('Unsupported or incomplete backup manifest')
    for name in NAMES:
        matches = [v for v in entries if v.get('name') == name]
        path = source / name
        if len(matches) != 1 or not path.is_file() or path.is_symlink():
            raise ValueError('Missing or duplicate backup file: '+name)
        if path.stat().st_size != matches[0]['bytes'] or digest(path).lower() != matches[0]['sha256'].lower():
            raise ValueError('Backup verification failed: '+name)
    # Only regular files/directories, relative paths, no symlinks/hardlinks/devices.
    with tarfile.open(source / 'images.tar.gz', 'r:gz') as archive:
        for entry in archive:
            path = PurePosixPath(entry.name)
            if path.is_absolute() or '..' in path.parts or not (entry.isfile() or entry.isdir()):
                raise ValueError('Unsafe archive member')
    return manifest

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('operation', choices=['backup', 'restore', 'verify'])
    parser.add_argument('--directory', required=True, help='New backup directory, or existing backup to restore/verify')
    parser.add_argument('--compose-file', default=str(Path(__file__).resolve().parent.parent / 'compose.yml'))
    parser.add_argument('--env-file', default=str(Path(__file__).resolve().parent.parent / '.env'))
    parser.add_argument('--project', default='chatgpt2api-go')
    parser.add_argument('--replace-existing', action='store_true', help='Explicitly replace target database and image contents')
    parser.add_argument('--no-start', action='store_true', help='Restore without starting core, useful for isolated recovery drills')
    args = parser.parse_args()
    directory = Path(args.directory).resolve()
    if args.operation == 'verify':
        verify(directory)
        print('Backup checksum and archive verification passed')
        return
    compose = Path(args.compose_file).resolve(strict=True)
    env = Path(args.env_file).resolve(strict=True)
    dc = ['docker', 'compose', '--env-file', str(env), '-f', str(compose), '-p', args.project]
    if args.operation == 'restore':
        verify(directory)  # Must finish before touching a target container.
        run(dc + ['up', '-d', '--wait', 'postgres'])
    pg = output(dc + ['ps', '-q', 'postgres'])
    if not pg:
        raise ValueError('PostgreSQL is not running')
    core = output(dc + ['ps', '-a', '-q', 'core'])
    if not core and args.operation == 'restore':
        run(dc + ['create', 'core'])
        core = output(dc + ['ps', '-a', '-q', 'core'])
    if not core:
        raise ValueError('Create the core container before backup')
    image = output(['docker', 'inspect', '--format', '{{.Image}}', core])
    running = output(['docker', 'inspect', '--format', '{{.State.Running}}', core]) == 'true'
    def volume(command, folder=None, writable=False):
        cmd = ['docker', 'run', '--rm', '--user', '0', '--entrypoint', '/bin/sh', '--volumes-from', core+('' if writable else ':ro')]
        if folder is not None:
            cmd += ['--mount', 'type=bind,source='+str(folder)+',target=/backup'+('' if writable else ',readonly')]
        return cmd + [image, '-ec', command]
    if args.operation == 'backup':
        directory.mkdir(mode=0o700, parents=False, exist_ok=False)
        complete = False
        try:
            if running:
                run(dc + ['stop', 'core'])
            with (directory / 'database.dump').open('wb') as f:
                run(database(pg, 'pg_dump -U "$POSTGRES_USER" -d "$POSTGRES_DB" -Fc'), stdout=f)
            # Stream from the volume; no helper image pull or host credential expansion.
            with (directory / 'images.tar.gz').open('wb') as f:
                run(volume('tar -czf - -C /data/images .'), stdout=f)
            (directory / 'environment.env').write_bytes(env.read_bytes())
            (directory / 'compose.yml').write_bytes(compose.read_bytes())
            manifest = {'version': 1, 'created_at': datetime.datetime.now(datetime.timezone.utc).isoformat(), 'project': args.project,
                        'image_id': image, 'files': [{'name': n, 'bytes': (directory/n).stat().st_size, 'sha256': digest(directory/n)} for n in NAMES]}
            (directory / 'manifest.json').write_text(json.dumps(manifest, indent=2)+'\n', encoding='utf8')
            for f in directory.iterdir():
                f.chmod(0o600)
            verify(directory)
            complete = True
            print('Backup complete: '+str(directory))
        finally:
            if running:
                run(dc + ['up', '-d', '--no-deps', 'core'])
            if not complete:
                print('Backup incomplete; do not restore from this directory', file=sys.stderr)
    else:
        if not args.replace_existing:
            tables = output(database(pg, 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -Atc "SELECT count(*) FROM pg_tables WHERE schemaname=\x27public\x27"'))
            if tables != '0':
                raise ValueError('Target database is not empty; use a fresh project')
            files = output(volume('find /data/images -mindepth 1 -maxdepth 1 | wc -l'))
            if files != '0':
                raise ValueError('Target image volume is not empty; use a fresh project')
        run(dc + ['stop', 'core'])
        temporary = '/tmp/imagecore-restore-'+uuid.uuid4().hex+'.dump'
        try:
            run(['docker', 'cp', str(directory / 'database.dump'), pg+':'+temporary])
            run(database(pg, 'pg_restore -U "$POSTGRES_USER" -d "$POSTGRES_DB" --clean --if-exists --no-owner --no-privileges --exit-on-error '+temporary))
            if args.replace_existing:
                # Fixed target: files in this target core's image volume only.
                run(volume('find /data/images -mindepth 1 -maxdepth 1 -exec rm -rf {} +', writable=True))
            with (directory / 'images.tar.gz').open('rb') as f:
                cmd = volume('tar -xzf - -C /data/images', writable=True)
                cmd.insert(3, '-i')
                run(cmd, stdin=f)
        except Exception:
            print('Restore failed; core remains stopped', file=sys.stderr)
            raise
        finally:
            run(['docker', 'exec', pg, 'rm', '-f', temporary])
        if not args.no_start:
            run(dc + ['up', '-d', '--no-deps', 'core'])
        print('Restore complete'+('; core left stopped' if args.no_start else '; check /readyz'))

if __name__ == '__main__':
    main()
