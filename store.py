"""Durable event-time tumbling windows with atomic, idempotent ingestion."""
import json
import math
import re
import sqlite3
from contextlib import contextmanager
from pathlib import Path


class Conflict(Exception):
    pass


class Missing(Exception):
    pass


def integer(value, name, low=0, high=2**40):
    if type(value) is not int or not low <= value <= high:
        raise ValueError(f'{name} must be an integer between {low} and {high}.')
    return value


def identifier(value, name):
    if not isinstance(value, str) or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.:-]{0,63}', value):
        raise ValueError(f'{name} must use 1–64 ASCII letters, digits, dots, underscores, colons, or hyphens; start with a letter or digit.')
    return value


def exact(body, fields):
    if not isinstance(body, dict) or set(body) != set(fields):
        raise ValueError('Expected exactly: '+', '.join(fields))


def event(value):
    exact(value, ['id', 'key', 'time', 'value'])
    identifier(value['id'], 'Event ID')
    identifier(value['key'], 'Key')
    integer(value['time'], 'Event time')
    number=value['value']
    if type(number) not in (int, float) or abs(number)>1e9 or not math.isfinite(number):
        raise ValueError('Value must be a finite number between -1e9 and 1e9.')
    return {'id': value['id'], 'key': value['key'], 'time': value['time'], 'value': float(number)}


def disposition(timestamp, width, lateness, watermark):
    start=timestamp//width*width
    if start+width+lateness <= watermark:
        return start, 'expired'
    return start, 'late' if timestamp < watermark else 'on-time'


class Store:
    def __init__(self, path):
        self.path=Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.connection() as db:
            db.executescript('''
                PRAGMA journal_mode=WAL;
                CREATE TABLE IF NOT EXISTS streams(
                    name TEXT PRIMARY KEY, width INTEGER NOT NULL, lateness INTEGER NOT NULL,
                    watermark INTEGER NOT NULL DEFAULT 0, revision INTEGER NOT NULL DEFAULT 0);
                CREATE TABLE IF NOT EXISTS events(
                    sequence INTEGER PRIMARY KEY AUTOINCREMENT, stream TEXT NOT NULL REFERENCES streams(name),
                    event_id TEXT NOT NULL, key TEXT NOT NULL, time INTEGER NOT NULL, value REAL NOT NULL,
                    start INTEGER NOT NULL, disposition TEXT NOT NULL, watermark INTEGER NOT NULL,
                    revision INTEGER NOT NULL, payload TEXT NOT NULL, UNIQUE(stream,event_id));
                CREATE INDEX IF NOT EXISTS event_windows ON events(stream,start,key);
                CREATE TABLE IF NOT EXISTS windows(
                    stream TEXT NOT NULL REFERENCES streams(name), start INTEGER NOT NULL, key TEXT NOT NULL,
                    count INTEGER NOT NULL, total REAL NOT NULL, minimum REAL NOT NULL, maximum REAL NOT NULL,
                    late_count INTEGER NOT NULL, PRIMARY KEY(stream,start,key));
                CREATE TABLE IF NOT EXISTS watermarks(
                    sequence INTEGER PRIMARY KEY AUTOINCREMENT, stream TEXT NOT NULL REFERENCES streams(name),
                    previous INTEGER NOT NULL, watermark INTEGER NOT NULL, revision INTEGER NOT NULL);
            ''')

    @contextmanager
    def connection(self, write=False):
        db=sqlite3.connect(self.path, timeout=5, isolation_level=None)
        db.row_factory=sqlite3.Row
        db.execute('PRAGMA foreign_keys=ON')
        try:
            db.execute('BEGIN IMMEDIATE' if write else 'BEGIN')
            yield db
            db.commit()
        except BaseException:
            db.rollback()
            raise
        finally:
            db.close()

    def _stream(self, db, name):
        identifier(name, 'Stream')
        row=db.execute('SELECT * FROM streams WHERE name=?', (name,)).fetchone()
        if row is None:
            raise Missing('Stream not found.')
        return dict(row)

    def create(self, body):
        exact(body, ['name', 'width', 'lateness'])
        name=identifier(body['name'], 'Stream')
        width=integer(body['width'], 'Window width', 1, 3600)
        lateness=integer(body['lateness'], 'Allowed lateness', 0, 3600)
        with self.connection(True) as db:
            if db.execute('SELECT 1 FROM streams WHERE name=?', (name,)).fetchone():
                raise Conflict('A stream with that name already exists.')
            if db.execute('SELECT count(*) FROM streams').fetchone()[0]>=20:
                raise ValueError('The local store is limited to 20 streams.')
            db.execute('INSERT INTO streams(name,width,lateness) VALUES(?,?,?)', (name,width,lateness))
            return self._stream(db,name)

    def ingest(self, name, body):
        exact(body, ['events'])
        if not isinstance(body['events'], list) or not 1<=len(body['events'])<=200:
            raise ValueError('Submit 1–200 events per atomic batch.')
        values=[event(e) for e in body['events']]
        with self.connection(True) as db:
            stream=self._stream(db,name)
            count=db.execute('SELECT count(*) FROM events WHERE stream=?',(name,)).fetchone()[0]
            receipts=[]
            revision=stream['revision']+1
            created=0
            for value in values:
                payload=json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False)
                prior=db.execute('SELECT * FROM events WHERE stream=? AND event_id=?',(name,value['id'])).fetchone()
                if prior:
                    if prior['payload']!=payload:
                        raise Conflict(f'Event ID {value["id"]} already belongs to a different payload; the entire batch was rolled back.')
                    receipts.append({'id':value['id'],'sequence':prior['sequence'],'disposition':prior['disposition'],'duplicate':True,'watermark':prior['watermark']})
                    continue
                if count+created>=10000:
                    raise ValueError('A stream is limited to 10,000 distinct event receipts.')
                start,decision=disposition(value['time'],stream['width'],stream['lateness'],stream['watermark'])
                cursor=db.execute('INSERT INTO events(stream,event_id,key,time,value,start,disposition,watermark,revision,payload) VALUES(?,?,?,?,?,?,?,?,?,?)',
                    (name,value['id'],value['key'],value['time'],value['value'],start,decision,stream['watermark'],revision,payload))
                if decision!='expired':
                    db.execute('''INSERT INTO windows VALUES(?,?,?,?,?,?,?,?)
                        ON CONFLICT(stream,start,key) DO UPDATE SET count=count+1,total=total+excluded.total,
                        minimum=min(minimum,excluded.minimum),maximum=max(maximum,excluded.maximum),late_count=late_count+excluded.late_count''',
                        (name,start,value['key'],1,value['value'],value['value'],value['value'],int(decision=='late')))
                receipts.append({'id':value['id'],'sequence':cursor.lastrowid,'disposition':decision,'duplicate':False,'watermark':stream['watermark']})
                created+=1
            if created:
                db.execute('UPDATE streams SET revision=? WHERE name=?',(revision,name))
            return {'created':created,'duplicates':len(values)-created,'receipts':receipts,'revision':revision if created else stream['revision']}

    def advance(self, name, body):
        exact(body,['watermark','expected_watermark'])
        target=integer(body['watermark'],'Watermark')
        expected=integer(body['expected_watermark'],'Expected watermark')
        with self.connection(True) as db:
            stream=self._stream(db,name)
            if expected!=stream['watermark']:
                raise Conflict('Watermark changed in another request. Reload the stream before advancing.')
            if target<stream['watermark']:
                raise ValueError('Watermarks cannot move backward.')
            if target>stream['watermark']:
                if db.execute('SELECT count(*) FROM watermarks WHERE stream=?',(name,)).fetchone()[0]>=10000:
                    raise ValueError('Watermark history is limited to 10,000 advances per stream.')
                db.execute('INSERT INTO watermarks(stream,previous,watermark,revision) VALUES(?,?,?,?)',(name,stream['watermark'],target,stream['revision']+1))
                db.execute('UPDATE streams SET watermark=?,revision=revision+1 WHERE name=?',(target,name))
            return self._stream(db,name)

    def list(self):
        with self.connection() as db:
            return [dict(r) for r in db.execute('SELECT * FROM streams ORDER BY name')]

    def snapshot(self, name):
        with self.connection() as db:
            stream=self._stream(db,name)
            windows=[]
            for row in db.execute('SELECT * FROM windows WHERE stream=? ORDER BY start,key',(name,)):
                item=dict(row)
                item['end']=item['start']+stream['width']
                item['final_at']=item['end']+stream['lateness']
                item['state']='final' if stream['watermark']>=item['final_at'] else 'grace' if stream['watermark']>=item['end'] else 'open'
                item['average']=item['total']/item['count']
                windows.append(item)
            counts={key:0 for key in ['on-time','late','expired']}
            counts.update({r[0]:r[1] for r in db.execute('SELECT disposition,count(*) FROM events WHERE stream=? GROUP BY disposition',(name,))})
            return {'stream':stream,'windows':windows,'counts':counts,
                'recent':[dict(r) for r in db.execute('SELECT sequence,event_id,key,time,value,start,disposition,watermark,revision FROM events WHERE stream=? ORDER BY sequence DESC LIMIT 50',(name,))]}

    def export(self,name):
        with self.connection() as db:
            return {'format':'window-foundry-log-v1','stream':self._stream(db,name),
                'events':[dict(r) for r in db.execute('SELECT sequence,event_id,key,time,value,start,disposition,watermark,revision FROM events WHERE stream=? ORDER BY sequence',(name,))],
                'watermarks':[dict(r) for r in db.execute('SELECT previous,watermark,revision FROM watermarks WHERE stream=? ORDER BY sequence',(name,))]}

    def audit(self,name,repair=False):
        with self.connection(repair) as db:
            stream=self._stream(db,name)
            expected={}
            for row in db.execute('SELECT * FROM events WHERE stream=? ORDER BY sequence',(name,)):
                start,decision=disposition(row['time'],stream['width'],stream['lateness'],row['watermark'])
                if start!=row['start'] or decision!=row['disposition']:
                    raise Conflict('Stored event receipt does not match its window decision. Refusing projection repair.')
                if decision=='expired':
                    continue
                key=(start,row['key'])
                if key not in expected:
                    expected[key]=[0,0.0,row['value'],row['value'],0]
                item=expected[key]
                item[0]+=1; item[1]+=row['value'];item[2]=min(item[2],row['value']);item[3]=max(item[3],row['value']);item[4]+=decision=='late'
            actual={(r['start'],r['key']):[r['count'],r['total'],r['minimum'],r['maximum'],r['late_count']] for r in db.execute('SELECT * FROM windows WHERE stream=?',(name,))}
            matches=actual==expected
            if repair and not matches:
                db.execute('DELETE FROM windows WHERE stream=?',(name,))
                db.executemany('INSERT INTO windows VALUES(?,?,?,?,?,?,?,?)',[(name,start,key,*values) for (start,key),values in expected.items()])
            return {'matched_before':matches,'repaired':repair and not matches,'windows':len(expected),'receipt_count':db.execute('SELECT count(*) FROM events WHERE stream=?',(name,)).fetchone()[0]}
