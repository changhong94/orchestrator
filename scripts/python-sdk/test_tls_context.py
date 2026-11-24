import concurrent.futures, threading, time, unittest, ssl, os, tempfile
from unittest.mock import patch
from e2b.api import client_sync as sdk
from e2b.connection_config import ConnectionConfig

class EnvdTLSContextTests(unittest.TestCase):
 def setUp(self):
  sdk._default_tls_contexts.clear()
  self.env=patch.dict(os.environ);self.env.start()
  os.environ.pop('SSL_CERT_FILE',None);os.environ.pop('SSL_CERT_DIR',None)
 def tearDown(self):self.env.stop();sdk._default_tls_contexts.clear()
 def test_concurrent_context_once_but_connections_remain_thread_local(self):
  barrier=threading.Barrier(32);calls=[];contexts=[]
  def factory(**kwargs):
   calls.append(1);time.sleep(.02);return ssl.create_default_context()
  def init(self,*args,**kw):contexts.append(kw['verify'])
  def get(_):barrier.wait();return sdk.get_envd_transport(ConnectionConfig())
  with patch('httpx._config.create_ssl_context',factory),patch.object(sdk.EnvdTransportWithLogger,'__init__',init):
   with concurrent.futures.ThreadPoolExecutor(32)as pool:out=list(pool.map(get,range(32)))
  self.assertEqual(len(calls),1)
  self.assertEqual(len(set(map(id,out))),32)
  self.assertEqual(len(set(map(id,contexts))),1)
  self.assertTrue(contexts[0].check_hostname)
  self.assertEqual(contexts[0].verify_mode,ssl.CERT_REQUIRED)
 def test_failure_not_cached(self):
  c=ssl.create_default_context()
  with patch('httpx._config.create_ssl_context',side_effect=[RuntimeError('injected'),c]):
   with self.assertRaises(RuntimeError):sdk._get_default_ssl_context(True)
   self.assertIs(sdk._get_default_ssl_context(True),c)
 def test_explicit_trust_env_uses_original_httpx_path(self):
  os.environ['SSL_CERT_FILE']='test-nondefault'
  with patch('httpx._config.create_ssl_context',side_effect=lambda **kwargs:object())as f:
   self.assertIsNot(sdk._get_default_ssl_context(True),sdk._get_default_ssl_context(True));self.assertEqual(f.call_count,2)
  self.assertEqual(sdk._default_tls_contexts,{})
 def test_http_protocol_and_ca_replacement_do_not_alias(self):
  with tempfile.TemporaryDirectory()as d:
   path=d+'/ca'
   with open(path,'w') as f:f.write('old')
   with patch('certifi.where',return_value=path),patch('httpx._config.create_ssl_context',side_effect=lambda **kwargs:object()):
    a=sdk._get_default_ssl_context(True);self.assertIs(a,sdk._get_default_ssl_context(True))
    self.assertIsNot(a,sdk._get_default_ssl_context(False))
    other=d+'/new'
    with open(other,'w') as f:f.write('new')
    os.replace(other,path)
    self.assertIsNot(a,sdk._get_default_ssl_context(True))
 def test_real_https_still_rejects_untrusted_server(self):
  import subprocess,http.server
  with tempfile.TemporaryDirectory()as d:
   subprocess.run(['openssl','req','-x509','-newkey','rsa:2048','-nodes','-keyout',d+'/key','-out',d+'/cert','-days','1','-subj','/CN=localhost','-addext','subjectAltName=DNS:localhost'],check=True,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
   ctx=ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER);ctx.load_cert_chain(d+'/cert',d+'/key')
   server=http.server.ThreadingHTTPServer(('127.0.0.1',0),http.server.BaseHTTPRequestHandler);server.socket=ctx.wrap_socket(server.socket,server_side=True)
   t=threading.Thread(target=server.serve_forever);t.start()
   import httpx
   try:
    with httpx.Client(transport=sdk.EnvdTransportWithLogger(verify=sdk._get_default_ssl_context(True),http2=True),timeout=3)as c:
     with self.assertRaises(httpx.ConnectError)as failure:c.get('https://localhost:'+str(server.server_port))
     self.assertIn('CERTIFICATE_VERIFY_FAILED',str(failure.exception))
   finally:server.shutdown();server.server_close();t.join()
if __name__=='__main__':unittest.main()
