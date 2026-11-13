import concurrent.futures
import json
import random
import sqlite3
import tempfile
import unittest
from pathlib import Path
from store import Store, Conflict, Missing


def item(id='e1', time=10, value=3, key='north'):
    return {'id':id,'time':time,'value':value,'key':key}


class WindowTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory()
        self.path=Path(self.temp.name)/'events.sqlite3'
        self.store=Store(self.path)
        self.store.create({'name':'s','width':60,'lateness':30})

    def tearDown(self):
        self.temp.cleanup()

    def send(self,*events):
        return self.store.ingest('s',{'events':list(events)})

    def advance(self,target,expected=0):
        return self.store.advance('s',{'watermark':target,'expected_watermark':expected})

    def test_half_open_boundaries_and_aggregation(self):
        self.send(item('a',0,5),item('b',59,-2),item('c',60,7))
        windows=self.store.snapshot('s')['windows']
        self.assertEqual([(w['start'],w['count'],w['total'],w['minimum'],w['maximum'],w['average']) for w in windows],[(0,2,3,-2,5,1.5),(60,1,7,7,7,7)])

    def test_watermark_grace_and_exact_expiration_boundary(self):
        self.send(item('a',10,5));self.advance(60)
        self.assertEqual(self.store.snapshot('s')['windows'][0]['state'],'grace')
        self.assertEqual(self.send(item('b',59,7))['receipts'][0]['disposition'],'late')
        self.advance(89,60)
        self.assertEqual(self.send(item('c',1,2))['receipts'][0]['disposition'],'late')
        self.advance(90,89)
        self.assertEqual(self.send(item('d',59,1000))['receipts'][0]['disposition'],'expired')
        state=self.store.snapshot('s');self.assertEqual(state['windows'][0]['total'],14);self.assertEqual(state['windows'][0]['state'],'final')
        self.assertEqual(state['counts'],{'on-time':1,'late':2,'expired':1})

    def test_event_behind_watermark_can_be_in_open_window(self):
        self.advance(20);self.assertEqual(self.send(item('a',10))['receipts'][0]['disposition'],'late')
        self.assertEqual(self.store.snapshot('s')['windows'][0]['state'],'open')

    def test_idempotent_receipt_survives_watermark_and_restart(self):
        original=self.send(item());self.advance(200)
        restarted=Store(self.path);receipt=restarted.ingest('s',{'events':[item()]})
        self.assertEqual(receipt['created'],0);self.assertTrue(receipt['receipts'][0]['duplicate'])
        self.assertEqual(receipt['receipts'][0]['disposition'],'on-time');self.assertEqual(receipt['receipts'][0]['sequence'],original['receipts'][0]['sequence'])
        self.assertEqual(restarted.snapshot('s')['windows'][0]['count'],1)

    def test_conflict_rolls_back_every_prior_item_in_batch(self):
        self.send(item('existing',10,5))
        with self.assertRaises(Conflict):self.send(item('new',20,8),item('existing',10,6))
        state=self.store.snapshot('s');self.assertEqual(len(state['recent']),1);self.assertEqual(state['windows'][0]['total'],5)
        self.assertEqual(state['stream']['revision'],1)

    def test_duplicate_within_batch_is_counted_once(self):
        result=self.send(item(),item())
        self.assertEqual((result['created'],result['duplicates']),(1,1));self.assertEqual(self.store.snapshot('s')['windows'][0]['count'],1)

    def test_concurrent_retries_have_one_effect(self):
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            results=list(pool.map(lambda _:Store(self.path).ingest('s',{'events':[item()]}),range(12)))
        self.assertEqual(sum(r['created'] for r in results),1);self.assertEqual(self.store.snapshot('s')['windows'][0]['count'],1)

    def test_stale_or_backward_watermark_is_rejected(self):
        self.advance(70)
        with self.assertRaises(Conflict):self.advance(80)
        with self.assertRaises(ValueError):self.advance(60,70)
        self.advance(70,70)
        self.assertEqual(len(self.store.export('s')['watermarks']),1)

    def test_expired_receipts_are_durable_deduplicated_and_do_not_create_windows(self):
        self.advance(1000);self.send(item());self.assertTrue(self.send(item())['receipts'][0]['duplicate'])
        self.assertEqual(self.store.snapshot('s')['windows'],[]);self.assertEqual(self.store.snapshot('s')['counts']['expired'],1)

    def test_projection_audit_and_repair_preserve_receipts(self):
        self.send(item('a',10,1),item('b',70,2));before=self.store.export('s')
        with self.store.connection(True) as db:db.execute('UPDATE windows SET total=999 WHERE start=0')
        self.assertFalse(self.store.audit('s')['matched_before']);self.assertTrue(self.store.audit('s',repair=True)['repaired'])
        self.assertTrue(self.store.audit('s')['matched_before']);self.assertEqual(self.store.export('s'),before)

    def test_sql_failure_rolls_back_receipt_and_projection(self):
        with self.store.connection(True) as db:db.execute("CREATE TRIGGER fail_insert BEFORE INSERT ON windows BEGIN SELECT RAISE(ABORT,'test failure'); END")
        with self.assertRaises(sqlite3.IntegrityError):self.send(item())
        self.assertEqual(self.store.export('s')['events'],[]);self.assertEqual(self.store.snapshot('s')['stream']['revision'],0)

    def test_500_out_of_order_events_match_independent_reference(self):
        rng=random.Random(9);expected={};counts={'on-time':0,'late':0,'expired':0};wm=0
        for i in range(500):
            if i%25==0:self.advance(wm+10,wm);wm+=10
            t=rng.randrange(300);value=rng.randrange(-20,21);key=rng.choice(['north','south']);e=item(str(i),t,value,key);self.send(e)
            start=(t//60)*60
            decision='expired' if start+90<=wm else 'late' if t<wm else 'on-time';counts[decision]+=1
            if decision!='expired':expected.setdefault((start,key),[]).append(value)
        state=self.store.snapshot('s');self.assertEqual(state['counts'],counts)
        for w in state['windows']:
            values=expected[(w['start'],w['key'])]
            self.assertEqual((w['count'],w['total'],w['minimum'],w['maximum']),(len(values),sum(values),min(values),max(values)))
        self.assertEqual(len(state['windows']),len(expected));self.assertTrue(self.store.audit('s')['matched_before'])

    def test_validation_and_stream_isolation(self):
        for bad in [item(time=True),item(value=float('nan')),item(value=10**1000),item(value=True),item(key='../x'),item(id=''),item(time=-1)]:
            with self.assertRaises(ValueError):self.send(bad)
        with self.assertRaises(ValueError):self.store.ingest('s',{'events':[]})
        with self.assertRaises(Missing):self.store.snapshot('missing')
        self.store.create({'name':'other','width':10,'lateness':0});self.send(item())
        self.store.ingest('other',{'events':[item()]})
        self.assertEqual(self.store.snapshot('other')['windows'][0]['start'],10)
        with self.assertRaises(Conflict):self.store.create({'name':'s','width':1,'lateness':0})


if __name__=='__main__':
    unittest.main()
