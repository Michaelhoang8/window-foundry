import json
import tempfile
import threading
import unittest
from http.client import HTTPConnection
from pathlib import Path
from run import make_server


class HTTPTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory()
        self.server=make_server(Path(self.temp.name)/'test.sqlite3')
        self.thread=threading.Thread(target=self.server.serve_forever,daemon=True)
        self.thread.start()

    def tearDown(self):
        self.server.shutdown();self.server.server_close();self.thread.join();self.temp.cleanup()

    def request(self,method,path,body=None,headers=None):
        conn=HTTPConnection(*self.server.server_address,timeout=3)
        try:
            payload=None if body is None else json.dumps(body)
            conn.request(method,path,payload,headers or ({'Content-Type':'application/json'} if body is not None else {}))
            response=conn.getresponse();raw=response.read()
            return response.status,json.loads(raw) if raw else None
        finally:conn.close()

    def test_create_ingest_finalize_export_and_conflict(self):
        self.assertEqual(self.request('POST','/api/streams',{'name':'s','width':60,'lateness':30})[0],201)
        event={'id':'one','key':'north','time':10,'value':5}
        self.assertEqual(self.request('POST','/api/streams/s/events',{'events':[event]})[1]['created'],1)
        self.assertEqual(self.request('POST','/api/streams/s/watermark',{'watermark':90,'expected_watermark':0})[0],200)
        self.assertEqual(self.request('GET','/api/streams/s')[1]['windows'][0]['state'],'final')
        self.assertTrue(self.request('POST','/api/streams/s/events',{'events':[event]})[1]['receipts'][0]['duplicate'])
        self.assertEqual(self.request('POST','/api/streams/s/events',{'events':[{**event,'value':6}]})[0],409)
        self.assertEqual(len(self.request('GET','/api/streams/s/export')[1]['events']),1)
        self.assertTrue(self.request('GET','/api/streams/s/audit')[1]['matched_before'])
        self.assertFalse(self.request('POST','/api/streams/s/rebuild',{})[1]['repaired'])
        self.assertEqual(self.request('HEAD','/api/streams/s')[0],200)

    def test_boundaries_and_unknown_routes(self):
        for path in ['/api/streams/missing','/.git/config','/data/windows.sqlite3','/../store.py']:
            self.assertEqual(self.request('GET',path)[0],404)
        self.assertEqual(self.request('GET','/api/streams',headers={'Host':'evil.invalid'})[0],400)
        self.assertEqual(self.request('POST','/api/streams',{},headers={'Content-Type':'application/json','Origin':'https://evil.invalid'})[0],403)
        self.assertEqual(self.request('POST','/api/streams',{},headers={'Content-Type':'text/plain'})[0],415)
        self.assertEqual(self.request('POST','/api/streams',[])[0],400)
        self.assertEqual(self.request('PUT','/api/streams',{})[0],405)
        self.assertEqual(self.request('POST','/api/streams',{'padding':'x'*64000})[0],413)


if __name__=='__main__':unittest.main()
