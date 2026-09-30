#!/usr/bin/env python3
"""Offline deployment regression tests. All cloud commands are replaced by fixtures."""
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]
DISHA = False
STUB = r"""#!/usr/bin/env python3
import json, os, pathlib, subprocess, sys
name = pathlib.Path(sys.argv[0]).name
args = sys.argv[1:]
with open(os.environ['COMMAND_LOG'], 'a') as f:
    f.write(json.dumps([name] + args) + '\n')
fail = os.environ.get('FAIL_COMMAND')
if fail and name == fail and (not os.environ.get('FAIL_MATCH') or os.environ['FAIL_MATCH'] in ' '.join(args)):
    sys.exit(37)
if name == 'git':
    if args[:2] == ['rev-parse', 'HEAD']: print('fixture-sha')
    elif args[:2] == ['rev-parse', '--abbrev-ref'] or args[:2] == ['branch', '--show-current']: print('main')
    elif args and args[0] == 'log': print('fixture-sha test')
    elif args[:2] == ['status', '--porcelain']: print(os.environ.get('DIRTY', ''), end='')
if name == 'jq':
    sys.exit(subprocess.call([os.environ['REAL_JQ']] + args))
if name == 'envsubst':
    sys.exit(subprocess.call([os.environ['REAL_ENVSUBST']] + args))
if name == 'kubectl':
    if 'apply' in args:
        with open(os.environ['MANIFEST_LOG'], 'a') as f: f.write(sys.stdin.read())
    elif args[:2] == ['config', 'get-contexts']:
        print('gke_curelinkai_us-east1_disha-voice-worker-staging')
    elif 'pods' in args and 'json' in args:
        print(json.dumps({'items': [{'metadata': {'name': 'healthy'}, 'status': {'phase': 'Running', 'containerStatuses': [{'ready': True}]}}, {'metadata': {'name': 'unhealthy'}, 'status': {'phase': 'Running', 'containerStatuses': [{'ready': False}]}}]}))
    elif 'create' in args:
        print('apiVersion: v1\nkind: Secret\nmetadata:\n  name: fixture')
"""


class DeployTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.bin = self.root / 'bin'
        self.bin.mkdir()
        for command in ['docker', 'aws', 'jq', 'gcloud', 'kubectl', 'alembic', 'envsubst', 'git', 'curl']:
            p = self.bin / command
            p.write_text(STUB.replace('#!/usr/bin/env python3', '#!' + sys.executable, 1))
            p.chmod(0o755)
        (self.bin / 'python').symlink_to(sys.executable)
        for filename in (['deploy-disha-backend.sh'] if DISHA else ['deploy-worker.sh', 'deploy-prod.sh', 'deploy-staging.sh']):
            shutil.copy(ROOT / filename, self.root / filename)
        shutil.copytree(ROOT / 'k8s', self.root / 'k8s')
        self.log = self.root / 'commands.jsonl'
        self.manifests = self.root / 'rendered.yaml'
        self.env = {
            'PATH': str(self.bin) + os.pathsep + os.environ['PATH'],
            'HOME': str(self.root),
            'GITHUB_ACTIONS': 'true', 'GITHUB_SHA': 'fixture-sha',
            'GITHUB_REF': 'refs/heads/main', 'GITHUB_ACTOR': 'fixture-user',
            'VIRTUAL_ENV': str(self.root / 'venv'), 'GCP_PROJECT_ID': 'curelinkai',
            'DEPLOY_IMAGE_TAG': 'fixture-sha-123-1', 'DEPLOY_POD_VERSION': 'gha-123-1',
            'COMMAND_LOG': str(self.log), 'MANIFEST_LOG': str(self.manifests),
            'REAL_ENVSUBST': shutil.which('envsubst') or '',
            'REAL_JQ': shutil.which('jq') or '',
        }
        self.assertTrue(self.env['REAL_JQ'], 'Install jq to run these tests')
        self.assertTrue(self.env['REAL_ENVSUBST'], 'Install gettext (envsubst) to run these tests')
        if DISHA:
            (self.root / 'common').mkdir()
            shutil.copy(ROOT / 'common/sqs_queue_mapping_manager.py', self.root / 'common/sqs_queue_mapping_manager.py')
            self.envfile = self.root / '.lambda.env'
            text = 'ENVIRONMENT=staging\nGKE_NAMESPACE=staging\nDISHA_BACKEND_CLUSTER_NAME=disha-backend-staging\nDISHA_BACKEND_CLUSTER_REGION=asia-south1\nAWS_MAIN_REGION=ap-south-1\nACCESS_KEY_ID=fixture-key\nSECRET_KEY_ID=fixture-secret\n'
            for queue in ['DEFAULT', 'PROCESS_MESSAGE_PARALLEL', 'FIFO_P0_FAST_L1']:
                text += f'SQS_{queue}_QUEUE_URL=https://example.test/{queue}\n'
            (self.root / 'system_env_manager.py').write_text("from pathlib import Path\nimport os, sys\nsys.exit(37) if os.environ.get('FAIL_FETCH') else None\nfor name in ['.lambda.env','.fly.env']: Path(name).write_text(Path('.fixture.env').read_text())\n")
        else:
            self.envfile = self.root / '.temp-deploy.env'
            text = 'ENVIRONMENT=staging\nDB_USER=fixture-user\nDB_PASSWORD=literal $value & spaces\nDB_HOST=example.test\nDB_PORT=5432\nDB_NAME=fixture-db\n'
            fetch = self.root / 'fetch-deploy-env.sh'
            fetch.write_text('#!/usr/bin/env bash\nset -e\n[[ -z "${FAIL_FETCH:-}" ]] || exit 37\ncp .fixture.env "$2"\n')
            fetch.chmod(0o755)
        self.envfile.write_text(text)
        (self.root / '.fixture.env').write_text(text)

    def run_phase(self, phase, environment='staging', **overrides):
        script = 'deploy-disha-backend.sh' if DISHA else 'deploy-worker.sh'
        return subprocess.run(['bash', str(self.root / script), environment, phase], cwd=self.root,
                              env={**self.env, **overrides}, capture_output=True, text=True, timeout=20)

    def commands(self):
        return [json.loads(line) for line in self.log.read_text().splitlines()] if self.log.exists() else []

    def test_build_uses_unique_tag_without_pushing(self):
        result = self.run_phase('build')
        self.assertEqual(result.returncode, 0, result.stderr)
        docker = [c for c in self.commands() if c[0] == 'docker']
        self.assertEqual(len(docker), 1)
        self.assertEqual(docker[0][1], 'build')
        self.assertIn(':fixture-sha-123-1', ' '.join(docker[0]))

    def test_build_failure_is_not_success(self):
        self.assertNotEqual(self.run_phase('build', FAIL_COMMAND='docker').returncode, 0)

    def test_push_failure_is_not_success(self):
        self.assertNotEqual(self.run_phase('push', FAIL_COMMAND='docker').returncode, 0)

    def test_failed_environment_fetch_stops_full_deploy(self):
        result = self.run_phase('all', FAIL_FETCH='1')
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse(any(c[0] == 'alembic' or c[:2] == ['docker', 'build'] for c in self.commands()))

    def test_production_branch_guard_runs_before_fetch(self):
        self.envfile.unlink()
        result = self.run_phase('prepare', environment='prod', GITHUB_REF='refs/heads/feature')
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('main', result.stdout + result.stderr)
        self.assertFalse(self.envfile.exists())

    def test_checked_out_commit_must_match_dispatch(self):
        self.assertNotEqual(self.run_phase('prepare', GITHUB_SHA='other-sha').returncode, 0)

    def test_render_failure_survives_successful_apply(self):
        phase = 'api' if DISHA else 'apply'
        self.assertNotEqual(self.run_phase(phase, FAIL_COMMAND='envsubst').returncode, 0)

    def test_apply_failure_is_not_success(self):
        phase = 'api' if DISHA else 'apply'
        self.assertNotEqual(self.run_phase(phase, FAIL_COMMAND='kubectl', FAIL_MATCH='apply').returncode, 0)

    def test_manifest_uses_exact_built_image(self):
        result = self.run_phase('api' if DISHA else 'apply')
        self.assertEqual(result.returncode, 0, result.stderr)
        rendered = self.manifests.read_text()
        self.assertIn(':fixture-sha-123-1', rendered)
        self.assertNotIn(':latest', rendered)
        self.assertIn('gha-123-1', rendered)
        self.assertNotIn('$GCP_PROJECT_ID', rendered)
        cluster = 'gke_curelinkai_asia-south1_disha-backend-staging' if DISHA else 'gke_curelinkai_us-east1_disha-voice-worker-staging'
        for command in self.commands():
            if command[0] == 'kubectl': self.assertIn(cluster, command)
        if not DISHA: self.assertIn('literal $value & spaces', rendered)

    def test_rollout_failure_is_not_success(self):
        match = 'background-worker-default' if DISHA else 'rollout'
        result = self.run_phase('rollout', FAIL_COMMAND='kubectl', FAIL_MATCH=match)
        self.assertNotEqual(result.returncode, 0)
        if DISHA: self.assertEqual(len([c for c in self.commands() if 'rollout' in c]), 4)

    def test_complete_deploy_preserves_build_push_apply_order(self):
        result = self.run_phase('all')
        self.assertEqual(result.returncode, 0, result.stderr)
        commands = self.commands()
        build = next(i for i, c in enumerate(commands) if c[:2] == ['docker', 'build'])
        push = next(i for i, c in enumerate(commands) if c[:2] == ['docker', 'push'])
        apply = next(i for i, c in enumerate(commands) if c[0] == 'kubectl' and 'apply' in c)
        self.assertLess(build, push)
        self.assertLess(push, apply)
        self.assertFalse(any(c[0] == 'curl' for c in commands), 'CI must not send Slack messages')

    def test_local_production_still_requires_clean_worktree(self):
        result = self.run_phase('prepare', environment='prod', GITHUB_ACTIONS='false', DIRTY=' M user-work.txt')
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('uncommitted', result.stdout + result.stderr)
        self.assertFalse(any(c[:2] == ['git', 'pull'] for c in self.commands()))

    def test_failure_diagnostics_only_log_unhealthy_pods_from_this_attempt(self):
        result = self.run_phase('diagnostics', DEPLOY_FAILED='true')
        self.assertEqual(result.returncode, 0, result.stderr)
        commands = self.commands()
        pod_queries = [c for c in commands if 'pods' in c and 'json' in c]
        self.assertEqual(len(pod_queries), 1)
        self.assertIn('pod-template-hash-binpack=gha-123-1', pod_queries[0])
        logs = [c for c in commands if 'logs' in c]
        self.assertEqual(len(logs), 2)
        self.assertTrue(all('unhealthy' in c and 'healthy' not in c for c in logs))
        self.assertTrue(any('--previous' in c for c in logs))

    def test_unavailable_container_logs_do_not_abort_diagnostics(self):
        for match in ['--previous', 'logs']:
            with self.subTest(match=match):
                result = self.run_phase('diagnostics', DEPLOY_FAILED='true', FAIL_COMMAND='kubectl', FAIL_MATCH=match)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertIn('::endgroup::', result.stdout)

    def test_success_diagnostics_do_not_read_application_logs(self):
        self.assertEqual(self.run_phase('diagnostics', DEPLOY_FAILED='false').returncode, 0)
        self.assertFalse(any('logs' in c for c in self.commands()))

    def test_invalid_phase_cannot_deploy(self):
        self.assertNotEqual(self.run_phase('typo').returncode, 0)
        self.assertEqual(self.commands(), [])


    def test_secret_creation_failure_survives_successful_apply(self):
        self.assertNotEqual(self.run_phase('secret', FAIL_COMMAND='kubectl', FAIL_MATCH='create secret').returncode, 0)

    def test_staging_cannot_target_production_cluster(self):
        with self.envfile.open('a') as stream: stream.write('GKE_CLUSTER_NAME=disha-voice-worker-prod\n')
        self.assertNotEqual(self.run_phase('apply').returncode, 0)
        self.assertEqual(self.commands(), [])

    def test_missing_database_value_is_rejected_before_secret_changes(self):
        self.envfile.write_text(self.envfile.read_text().replace('DB_NAME=fixture-db', 'DB_NAME='))
        self.assertNotEqual(self.run_phase('secret').returncode, 0)
        self.assertEqual(self.commands(), [])


if __name__ == '__main__':
    unittest.main()
