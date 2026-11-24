import concurrent.futures,threading,time,unittest
from unittest.mock import patch
from e2b.api import client_sync as sdk
from e2b.connection_config import ConnectionConfig

class TransportInitTests(unittest.TestCase):
 def setUp(self): sdk.TransportWithLogger._instances.clear()
 def tearDown(self): sdk.TransportWithLogger._instances.clear()
 def test_concurrent_first_use_constructs_one_pool(self):
  barrier=threading.Barrier(64);calls=[]
  def init(self,*a,**kw):calls.append(kw);time.sleep(.02)
  def get(_):barrier.wait();return sdk.get_transport(ConnectionConfig())
  with patch.object(sdk.TransportWithLogger,'__init__',init):
   with concurrent.futures.ThreadPoolExecutor(64)as pool:results=list(pool.map(get,range(64)))
  self.assertEqual(len(calls),1)
  self.assertTrue(all(x is results[0]for x in results))
  import ssl
  self.assertTrue(calls[0]['verify'].check_hostname)
  self.assertEqual(calls[0]['verify'].verify_mode,ssl.CERT_REQUIRED)
 def test_failed_initialization_can_retry(self):
  calls=[]
  def init(self,*a,**kw):
   calls.append(kw)
   if len(calls)==1:raise RuntimeError('injected failure')
  with patch.object(sdk.TransportWithLogger,'__init__',init):
   with self.assertRaises(RuntimeError):sdk.get_transport(ConnectionConfig())
   second=sdk.get_transport(ConnectionConfig())
   self.assertIs(second,sdk.get_transport(ConnectionConfig()))
  self.assertEqual(len(calls),2)
 def test_http_versions_keep_separate_pools(self):
  with patch.object(sdk.TransportWithLogger,'__init__',lambda *a,**kw:None):
   self.assertIsNot(sdk.get_transport(ConnectionConfig(),True),sdk.get_transport(ConnectionConfig(),False))
if __name__=='__main__':unittest.main()
