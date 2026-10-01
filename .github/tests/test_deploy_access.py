"""Execute the real workflow preflight without cloud credentials or deployment."""
import os
from pathlib import Path
import subprocess
import textwrap
import unittest

ROOT = Path(__file__).resolve().parents[2]
WORKFLOW = (ROOT / '.github/workflows/deploy-k8s.yml').read_text()
PREFLIGHT = textwrap.dedent(WORKFLOW.split('        run: |\n', 1)[1].split('\n      - name:', 1)[0])
USERS = [('20072704', 'jaideep329'), ('43138335', 'ManasviPatidar'), ('54813606', 'Waheguru-Anurag')]

class DeploymentAccessTests(unittest.TestCase):
    def check_access(self, allowed, actor_id='', triggering_actor='', environment='prod', ref='refs/heads/main'):
        env = {'PATH': os.environ['PATH'], 'DEPLOY_ENVIRONMENT': environment,
               'GITHUB_ACTOR_ID': actor_id, 'GITHUB_TRIGGERING_ACTOR': triggering_actor,
               'GITHUB_REF': ref, 'AWS_ROLE_ARN': 'test',
               'GCP_WORKLOAD_IDENTITY_PROVIDER': 'test', 'GCP_SERVICE_ACCOUNT': 'test'}
        result = subprocess.run(['bash', '-e', '-o', 'pipefail', '-c', PREFLIGHT], env=env, capture_output=True, text=True)
        self.assertEqual(result.returncode == 0, allowed, result.stdout + result.stderr)

    def test_each_approved_dispatcher_and_rerunner(self):
        for actor_id, actor in USERS:
            for _, initiator in USERS:
                with self.subTest(actor=actor, initiator=initiator):
                    self.check_access(True, actor_id, initiator)

    def test_other_dispatcher_cannot_deploy_even_with_approved_rerunner(self):
        self.check_access(False, '99999999', 'jaideep329')

    def test_other_rerunner_cannot_replay_approved_dispatch(self):
        self.check_access(False, USERS[0][0], 'other-collaborator')

    def test_missing_actor_fails_closed(self):
        self.check_access(False, '', 'jaideep329')
        self.check_access(False, USERS[0][0], '')

    def test_staging_has_no_actor_allowlist_and_accepts_feature_branches(self):
        self.check_access(True, '99999999', 'other-collaborator', 'staging', 'refs/heads/feature/example')
        self.check_access(True, environment='staging')

    def test_production_requires_main_even_for_approved_users(self):
        for ref in ['refs/heads/feature/example', 'refs/tags/main']:
            self.check_access(False, *USERS[0], ref=ref)

    def test_invalid_environment_is_rejected(self):
        self.check_access(False, *USERS[0], environment='production')

if __name__ == '__main__':
    unittest.main()
