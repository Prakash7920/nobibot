"""Experimental read-only adapter, pinned to the inspected PyQuotex revision.
No buy/sell calls. No silent normal/OTC substitution. Credentials stay in process.
PyQuotex itself may cache a session locally; see README before using it.
"""
import asyncio
import math
import time


def normalize_candles(rows, period):
    result = {}
    if not isinstance(rows, list):
        raise ValueError('Unexpected candle response schema')
    for row in rows:
        if not isinstance(row, dict):
            raise ValueError('Unexpected candle item schema')
        ts = row.get('time', row.get('start_time', row.get('timestamp')))
        c = {k: float(row[k]) for k in ('open', 'high', 'low', 'close')}
        c['time'] = int(float(ts))
        c['volume'] = float(row.get('ticks', row.get('volume', 0)))
        if not all(math.isfinite(v) for v in c.values()) or min(c[k] for k in ('open','high','low','close')) <= 0:
            raise ValueError('Invalid candle values')
        if c['high'] < max(c['open'],c['close']) or c['low'] > min(c['open'],c['close']):
            raise ValueError('Invalid OHLC range')
        result[c['time']] = c
    return sorted(result.values(), key=lambda c:c['time'])


async def run(engine, email, password, stop_event):
    try:
        from pyquotex.stable_api import Quotex
        from pyquotex.utils.account_type import AccountType
    except ImportError:
        with engine.lock:
            engine.log('ERROR', 'Optional connector missing. Run INSTALL_CONNECTOR.bat first.')
        return
    client = None
    try:
        # Deliberately no debug logging or raw exception text (could contain secrets).
        client = Quotex(email=email, password=password, lang='en', host='quotex.com')
        client.set_account_mode('PRACTICE')
        ok, _ = await asyncio.wait_for(client.connect(), 90)
        if not ok:
            raise ConnectionError('Authentication rejected')
        if client.account_is_demo != AccountType.DEMO or client.api.account_type != AccountType.DEMO:
            raise ValueError('Demo account mode could not be verified')
        instruments = await asyncio.wait_for(client.get_instruments(), 25)
        markets = []
        for row in instruments:
            if len(row) < 19: continue
            payouts = client.get_payout_by_asset(row[1], 'all') or {}
            markets.append(dict(id=str(row[1]), name=str(row[2]).replace('\n',''), open=bool(row[14]), payouts=payouts))
        if not markets: raise ValueError('No supported instrument response')
        balance = await asyncio.wait_for(client.get_balance(), 20)
        with engine.lock:
            engine.assert_idle()
            engine.mode = 'broker'
            engine.running = False
            engine.config['auto'] = False
            engine.markets = markets
            engine.broker_balance = balance
            if engine.config['asset'] not in [m['id'] for m in markets]:
                engine.config['asset'] = next((m['id'] for m in markets if m['open']), markets[0]['id'])
            engine.rows=[]; engine.candles=[]; engine.last_tick=0
            engine.log('INFO', 'Connected to Quotex practice feed through unofficial adapter. Orders remain locked.')
        active = None
        history=[]
        last_ts=0
        while not stop_event.is_set():
            if not await client.check_connect():
                raise ConnectionError('Feed disconnected')
            with engine.lock:
                if engine.mode != 'broker': break
                key=(engine.config['asset'], engine.config['timeframe'])
            if key != active:
                if active:
                    await client.stop_candles_stream(active[0])
                raw=await asyncio.wait_for(client.get_candles(key[0], time.time(), key[1]*180, key[1]),30)
                history=normalize_candles(raw, key[1])
                await client.start_candles_stream(key[0], key[1])
                active=key; last_ts=0
                with engine.lock:
                    engine.candles=history[-200:]
                    engine.last_analyzed=next((c["time"] for c in reversed(history) if c["time"]+key[1]<=time.time()), None)
            prices=await client.get_realtime_price(key[0])
            for tick in prices:
                ts, price=float(tick['time']),float(tick['price'])
                if ts<=last_ts: continue
                if not math.isfinite(price) or price<=0: continue
                # Reject old/future ticks; local clock must be synchronized.
                if not -5 <= time.time()-ts <= 15: continue
                last_ts=ts
                bucket=int(ts//key[1])*key[1]
                if history and bucket<history[-1]['time']: continue
                if not history or bucket>history[-1]['time']:
                    history.append(dict(time=bucket,open=price,high=price,low=price,close=price,volume=1))
                else:
                    c=history[-1]; c['high']=max(c['high'],price); c['low']=min(c['low'],price); c['close']=price; c['volume']+=1
                history=history[-250:]
                with engine.lock:
                    if key != (engine.config['asset'],engine.config['timeframe']): break
                    engine.candles=[c for c in history if c['time']+key[1]<=ts]
                    engine.feed_time=ts
                    engine.last_tick=time.time()
                    if engine.running:
                        # Reject gaps before generating a signal.
                        recent=engine.candles[-35:]
                        continuous=all(b['time']-a['time']==key[1] for a,b in zip(recent,recent[1:]))
                        if continuous and ts-bucket<=5:
                            before=len(engine.signals)
                            engine.evaluate(ts,price)
                            if len(engine.signals)!=before: engine.save()
                        elif continuous: engine.last_analyzed=engine.candles[-1]["time"] if engine.candles else None
                        else: engine.analysis=dict(direction='WAIT', reason='Gap in broker candles', votes={},rsi=None)
                    if not engine.running and engine.candles:
                        engine.last_analyzed=engine.candles[-1]["time"]
                    # Current unfinished candle is visible, never used for analysis.
                    engine.candles=history[:]
            with engine.lock:
                if engine.last_tick and time.time()-engine.last_tick>15 and engine.running:
                    engine.running=False
                    engine.log('ERROR','Stale feed: signal engine paused. Resume manually after recovery.')
            await asyncio.sleep(.5)
        await client.close()
    except Exception as exc:
        with engine.lock:
            engine.running=False
            engine.last_tick=0
            engine.log('ERROR', 'Broker connection stopped ('+type(exc).__name__+'). Check local login, account mode, dependency version, and connectivity. No automatic order retry.')
            engine.save()
    finally:
        if client:
            try: await client.close()
            except Exception: pass
