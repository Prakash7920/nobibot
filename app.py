"""Loopback-only NOBI TRADE app. Standard library UI server; no CDN dependencies."""
import argparse
import asyncio
import csv
import getpass
import io
import json
import logging
import os
import queue
import secrets
import threading
import time
import urllib.request
import webbrowser
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from core import Engine, backtest, parse_csv, format_signal

ROOT=Path(__file__).resolve().parent
DATA=ROOT/'data'
TOKEN=secrets.token_urlsafe(32)
engine=None
stop_event=threading.Event()
telegram_queue=queue.Queue(maxsize=100)


def telegram_worker():
    while not stop_event.is_set():
        try: message=telegram_queue.get(timeout=1)
        except queue.Empty: continue
        token=os.environ.get('NOBI_TELEGRAM_TOKEN','')
        chat=os.environ.get('NOBI_TELEGRAM_CHAT_ID','')
        try:
            if not token or not chat: raise ValueError('Missing configuration')
            payload=json.dumps(dict(chat_id=chat,text=message[:4000])).encode()
            req=urllib.request.Request('https://api.telegram.org/bot'+token+'/sendMessage',data=payload,headers={'Content-Type':'application/json'})
            with urllib.request.urlopen(req,timeout=10) as response:
                result=json.load(response)
            if not result.get('ok'): raise ValueError('Rejected')
            with engine.lock: engine.log('INFO','Telegram message delivered')
        except Exception:
            with engine.lock: engine.log('ERROR','Telegram delivery failed; not retried to avoid duplicate alerts. Check token/chat/network locally.')
        finally: telegram_queue.task_done()


def ticker():
    while not stop_event.wait(1):
        with engine.lock:
            if engine.running and engine.mode in ('sample','csv'): engine.step()
            while engine.outbox:
                msg=engine.outbox.pop(0)
                try: telegram_queue.put_nowait(msg)
                except queue.Full: engine.log('ERROR','Telegram queue full; alert dropped')


class Handler(BaseHTTPRequestHandler):
    def log_message(self,*args): pass

    def reply(self,status,data,ctype='application/json'):
        body=json.dumps(data,allow_nan=False).encode() if ctype=='application/json' else data
        self.send_response(status)
        self.send_header('Content-Type',ctype)
        self.send_header('Content-Length',str(len(body)))
        self.send_header('Cache-Control','no-store')
        self.send_header('X-Content-Type-Options','nosniff')
        self.send_header('Content-Security-Policy',"default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self' data:; connect-src 'self'; frame-ancestors 'none'")
        self.end_headers(); self.wfile.write(body)

    def allowed(self):
        return self.headers.get('Host') in (f'127.0.0.1:{self.server.server_port}',f'localhost:{self.server.server_port}')

    def do_GET(self):
        if not self.allowed(): return self.reply(403,{'error':'Invalid host'})
        path=self.path.split('?')[0]
        if path=='/api/state':
            with engine.lock:
                data=engine.snapshot()
                data['telegram_configured']=bool(os.environ.get('NOBI_TELEGRAM_TOKEN') and os.environ.get('NOBI_TELEGRAM_CHAT_ID'))
                data['token']=TOKEN
                return self.reply(200,data)
        if path=='/api/export':
            with engine.lock:
                s=io.StringIO(); fields=['id','source','asset','time','direction','entry','expiry_at','stake','payout','status','pnl']
                w=csv.DictWriter(s,fieldnames=fields,extrasaction='ignore'); w.writeheader();w.writerows(engine.trades)
            return self.reply(200,s.getvalue().encode(),'text/csv')
        files={'/':'index.html','/app.js':'app.js','/style.css':'style.css'}
        if path in files:
            f=ROOT/'web'/files[path]
            ct='text/html; charset=utf-8' if path=='/' else 'text/javascript' if path.endswith('.js') else 'text/css'
            return self.reply(200,f.read_bytes(),ct)
        return self.reply(404,{'error':'Not found'})

    def do_POST(self):
        if not self.allowed() or self.headers.get('X-Nobi-Token')!=TOKEN:
            return self.reply(403,{'error':'Invalid local request token'})
        origin=self.headers.get('Origin')
        if origin and origin not in (f'http://127.0.0.1:{self.server.server_port}',f'http://localhost:{self.server.server_port}'):
            return self.reply(403,{'error':'Invalid origin'})
        try:
            length=int(self.headers.get('Content-Length','0'))
            if not 0<length<=5_000_000: raise ValueError('Invalid request size')
            data=json.loads(self.rfile.read(length))
            with engine.lock:
                action=self.path
                result={'ok':True}
                if action=='/api/settings': engine.update_config(data)
                elif action=='/api/sample':
                    if engine.mode=='broker': raise ValueError('Restart without --quotex before switching to sample replay')
                    engine.load_replay()
                elif action=='/api/import':
                    if engine.mode=='broker': raise ValueError('Restart without --quotex before importing replay')
                    rows,period=parse_csv(data['csv']);engine.load_replay(rows,period)
                elif action=='/api/start':
                    if engine.emergency: raise ValueError('Clear emergency stop first')
                    if not engine.candles: raise ValueError('No feed. Load a replay or start the optional broker connector')
                    if engine.mode=='broker' and (not engine.last_tick or time.time()-engine.last_tick>15): raise ValueError('Broker feed is stale or not connected')
                    if engine.mode in ('sample','csv') and engine.cursor>=len(engine.rows)-1: raise ValueError('End of replay; load another dataset')
                    engine.running=True;engine.log('INFO','Signal engine started')
                elif action=='/api/stop':
                    engine.running=False;engine.config['auto']=False;engine.log('INFO','Engine paused; open paper trades settle when replay resumes')
                elif action=='/api/emergency':
                    engine.emergency=True;engine.config['auto']=False;engine.running=False;engine.log('WARN','Emergency stop: no new trades. Open replay positions need Step/Resume to settle.')
                elif action=='/api/clear': engine.emergency=False;engine.log('INFO','Emergency stop cleared; engine remains paused')
                elif action=='/api/step':
                    if engine.mode not in ('sample','csv'): raise ValueError('Step is available only in replay mode')
                    engine.step()
                elif action=='/api/order': result=engine.manual_order(data['direction'])
                elif action=='/api/backtest':
                    if engine.mode not in ('sample','csv') or not engine.rows: raise ValueError('Load sample or CSV replay first')
                    if engine.running: raise ValueError('Pause engine before backtesting')
                    result=backtest(engine.rows,engine.config['timeframe'],engine.config)
                elif action=='/api/preview':
                    sig=dict(id='preview',asset=engine.config['asset'],time=time.time(),expiry=engine.config['expiry'],direction='CALL',reason='EXAMPLE ONLY',source='EXAMPLE')
                    result={'text':format_signal(sig,data.get('template',engine.config['template']),data.get('custom',engine.config['custom']))}
                elif action=='/api/telegram-test':
                    if not os.environ.get('NOBI_TELEGRAM_TOKEN') or not os.environ.get('NOBI_TELEGRAM_CHAT_ID'): raise ValueError('Configure Telegram environment variables locally first')
                    telegram_queue.put_nowait('NOBI TRADE: connection test. No trade signal.')
                else: return self.reply(404,{'error':'Unsupported action'})
                engine.save()
            return self.reply(200,result)
        except (ValueError,KeyError,TypeError,queue.Full) as exc:
            return self.reply(400,{'error':str(exc)})
        except Exception as exc:
            with engine.lock: engine.log('ERROR','Request failed: '+type(exc).__name__)
            return self.reply(500,{'error':'Operation failed. See logs; no order retry was performed.'})


def main():
    global engine
    p=argparse.ArgumentParser();p.add_argument('--port',type=int,default=8765);p.add_argument('--no-browser',action='store_true');p.add_argument('--quotex',action='store_true');args=p.parse_args()
    DATA.mkdir(exist_ok=True)
    engine=Engine(str(DATA/'nobi.sqlite3'))
    logging.disable(logging.CRITICAL) # Third-party logs can contain session data.
    server=ThreadingHTTPServer(('127.0.0.1',args.port),Handler)
    threading.Thread(target=ticker,daemon=True).start()
    threading.Thread(target=telegram_worker,daemon=True).start()
    if args.quotex:
        print('Experimental READ-ONLY Quotex practice connector. No broker orders.\nCredentials are entered only in this local terminal. PyQuotex may cache a local session.\nIf login asks for OTP, enter it here, never in chat.')
        email=input('Quotex email: ').strip();password=getpass.getpass('Quotex password: ')
        from broker import run
        threading.Thread(target=lambda:asyncio.run(run(engine,email,password,stop_event)),daemon=True).start()
    url=f'http://127.0.0.1:{args.port}'
    print('NOBI TRADE: '+url+'\nKeep this window open. Ctrl+C stops the app.')
    if not args.no_browser: webbrowser.open(url)
    try: server.serve_forever()
    except KeyboardInterrupt: pass
    finally:
        stop_event.set();server.server_close()
        with engine.lock: engine.save()

if __name__=='__main__':main()
