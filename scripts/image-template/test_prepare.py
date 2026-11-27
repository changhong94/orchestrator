import io
import json
import sys
import types
import unittest
from unittest.mock import patch

# HTTP publication tests do not require an installed E2B SDK.
with patch.dict(sys.modules, {'e2b': types.SimpleNamespace(Sandbox=object)}):
    import prepare


class PausePublicationTest(unittest.TestCase):
    def test_pause_precedes_export_and_restore_resets_identity(self):
        replies = [io.BytesIO(b'{}'), io.BytesIO(b'{"result":"template-test"}')]
        with patch.object(prepare.urllib.request, 'urlopen', side_effect=replies) as call:
            result = prepare.pause_and_publish({'api': 'http://test', 'key': 'key'}, 'seed')
        requests = [x.args[0] for x in call.call_args_list]
        self.assertEqual([r.full_url for r in requests], [
            'http://test/sandboxes/seed/pause', 'http://test/sandboxes/seed/export'])
        self.assertEqual(json.loads(requests[0].data), {'memory': True})
        self.assertEqual(json.loads(requests[1].data), {'toTemplate': True, 'keepSource': True})
        self.assertEqual(result['templateID'], 'template-test')
        self.assertTrue(json.loads(result['metadata']['kuasar-sandbox.restore'])['fork_identity'])

    def test_pause_failure_does_not_publish(self):
        with patch.object(prepare.urllib.request, 'urlopen', side_effect=OSError('pause failed')) as call:
            with self.assertRaises(OSError):
                prepare.pause_and_publish({'api': 'http://test', 'key': 'key'}, 'seed')
        self.assertEqual(call.call_count, 1)

    def test_missing_template_is_rejected(self):
        with patch.object(prepare.urllib.request, 'urlopen', side_effect=[io.BytesIO(b'{}'), io.BytesIO(b'{}')]):
            with self.assertRaises(RuntimeError):
                prepare.pause_and_publish({'api': 'http://test', 'key': 'key'}, 'seed')
