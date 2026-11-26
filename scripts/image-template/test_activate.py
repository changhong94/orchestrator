import base64
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from activate_catalogs import activate


class ActivationTest(unittest.TestCase):
    def test_publish_together_and_preserve_other_settings(self):
        self.exercise(False)

    def test_invalid_second_catalog_leaves_config_unchanged(self):
        self.exercise(True)

    def exercise(self, invalid):
        with tempfile.TemporaryDirectory() as root:
            root = Path(root)
            credential = root / 'credential.json'
            credential.write_text(json.dumps({'key': 'test-only'}))
            config = root / 'config.json'
            before = {'checkpoint': {'snapshot_target': 'manifest'},
                      'units': {'runner_pools': [{'name': 'prepared', 'prepared_vm': True,
                      'prepared_restore': False, 'size': 64, 'prepare_concurrency': 8}]}}
            config.write_text(json.dumps(before)); config.chmod(0o600)
            raw = config.read_bytes()
            catalogs = []
            for i in range(2):
                reference = 'manifest://invalid' if invalid and i == 1 else 'file://snapshot'
                path = root / f'{i}.json'
                path.write_text(json.dumps({
                    'application_started': False, 'language_specific_preparation': False,
                    'identity': {'image_template': f'image-{i}', 'credential_fingerprint': hashlib.sha256(b'test-only').hexdigest()},
                    'template': {'templateID': 'e2b-snp-' + base64.urlsafe_b64encode(reference.encode()).decode().rstrip('='),
                                 'metadata': {'kuasar-sandbox.restore': '{"fork_identity":true}'}},
                    'host_preflight_version': 1, 'staging_credential_file': str(credential),
                    'cache': {'errors': 0, 'application_executed': False}}))
                catalogs.append(path)
            if invalid:
                with self.assertRaisesRegex(ValueError, 'requires a local snapshot'):
                    activate(config, catalogs, credential, 'prepared')
                self.assertEqual(config.read_bytes(), raw)
            else:
                activate(config, catalogs, credential, 'prepared')
                after = json.loads(config.read_text())
                self.assertEqual(after['checkpoint'], before['checkpoint'])
                self.assertEqual(after['units']['runner_pools'][0]['size'], 64)
                self.assertEqual(after['units']['runner_pools'][0]['prepare_concurrency'], 8)
                self.assertTrue(after['units']['runner_pools'][0]['prepared_restore'])
                self.assertEqual(len(after['image_template_catalogs']), 2)
                self.assertEqual(config.stat().st_mode & 0o777, 0o600)


if __name__ == '__main__':
    unittest.main()
