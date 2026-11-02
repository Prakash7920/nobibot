"""NOBI TRADE research engine. No broker order execution in this release."""
import csv
import io
import json
import math
import random
import sqlite3
import statistics
import threading
import time
from datetime import datetime, timezone

DEFAULTS = dict(asset='SAMPLE_EURUSD', timeframe=60, expiry=120, stake=1.0,
                payout=80.0, daily_loss=10.0, max_trades=20, loss_streak=3,
                strategies=['trend', 'momentum'], auto=False, telegram=False,
                template='detailed', custom='NOBI TRADE\n{asset} {arrow} {direction}\nEntry: {entry}\nExpiry: {expiry}s\n{reason}')


def number(value, low, high):
    x = float(value)
    if not math.isfinite(x) or not low <= x <= high:
        raise ValueError(f'Value must be between {low} and {high}')
    return x


def ema(values, length):
    e = values[0]
    for x in values[1:]:
        e += 2 / (length + 1) * (x - e)
    return e


def analyze(candles, enabled):
    if len(candles) < 35:
        return dict(direction='WAIT', reason='Need 35 closed candles', votes={}, rsi=None)
    closes = [c['close'] for c in candles[-100:]]
    diffs = [b-a for a, b in zip(closes[-15:-1], closes[-14:])]
    gain = sum(max(d, 0) for d in diffs) / len(diffs)
    loss = sum(max(-d, 0) for d in diffs) / len(diffs)
    rsi = 100 - 100 / (1 + gain/loss) if loss else (100 if gain else 50)
    recent = candles[-14:]
    atr = statistics.mean(c['high']-c['low'] for c in recent)
    if not atr or candles[-1]['high']-candles[-1]['low'] > 3 * atr:
        return dict(direction='WAIT', reason='Flat prices or oversized candle: volatility filter', votes={}, rsi=round(rsi, 1))
    fast, slow = ema(closes, 9), ema(closes, 21)
    votes = {}
    if 'trend' in enabled:
        votes['trend'] = 'CALL' if fast > slow and closes[-1] > fast else 'PUT' if fast < slow and closes[-1] < fast else 'WAIT'
    if 'momentum' in enabled:
        votes['momentum'] = 'CALL' if 52 < rsi < 75 else 'PUT' if 25 < rsi < 48 else 'WAIT'
    if 'breakout' in enabled:
        votes['breakout'] = 'CALL' if closes[-1] > max(c['high'] for c in candles[-11:-1]) else 'PUT' if closes[-1] < min(c['low'] for c in candles[-11:-1]) else 'WAIT'
    direction = next(iter(votes.values())) if votes and len(set(votes.values())) == 1 else 'WAIT'
    reason = '; '.join(k + ': ' + v for k, v in votes.items()) or 'No strategy selected'
    return dict(direction=direction, reason=reason, votes=votes, rsi=round(rsi, 1))


def parse_csv(text):
    rows = list(csv.DictReader(io.StringIO(text)))
    if not 40 <= len(rows) <= 20000:
        raise ValueError('CSV needs 40–20,000 rows')
    result = []
    for row in rows:
        c = {k: number(row[k], 0.000000001, 1e15) for k in ('open', 'high', 'low', 'close')}
        c['time'] = int(number(row['time'], 1, 4102444800))
        c['volume'] = number(row.get('volume') or 0, 0, 1e15)
        if c['low'] > min(c['open'], c['close']) or c['high'] < max(c['open'], c['close']) or c['low'] > c['high']:
            raise ValueError('Invalid OHLC range')
        if result and c['time'] <= result[-1]['time']:
            raise ValueError('Times must be strictly increasing Unix seconds')
        result.append(c)
    step = result[1]['time']-result[0]['time']
    if step not in (60, 120, 300, 900) or any(b['time']-a['time'] != step for a, b in zip(result, result[1:])):
        raise ValueError('Use continuous 1m, 2m, 5m or 15m candles; fill no gaps artificially')
    return result, step


def sample(asset, period, n=900):
    rng = random.Random(asset + str(period))
    price = 83.5 if 'INR' in asset else 1.08
    scale = price * .0005
    start = 1767225600
    rows = []
    for i in range(n):
        o = price
        price = max(.001, o + scale * (rng.gauss(0, .8) + .45 * math.sin(i/18)))
        rows.append(dict(time=start+i*period, open=o, close=price,
                         high=max(o, price)+rng.random()*scale, low=min(o, price)-rng.random()*scale,
                         volume=rng.randint(10, 120)))
    return rows


def format_signal(signal, template, custom):
    arrow = '🟢 ▲' if signal['direction'] == 'CALL' else '🔴 ▼'
    fields = dict(asset=signal['asset'], arrow=arrow, direction=signal['direction'],
                  entry=datetime.fromtimestamp(signal['time'], timezone.utc).isoformat(),
                  expiry=signal['expiry'], reason=signal['reason'], source=signal['source'], id=signal['id'])
    formats = {
        'compact': 'NOBI TRADE | {source}\n{asset} {arrow} {direction}\n{entry} | {expiry}s',
        'detailed': 'NOBI TRADE | {source}\nAsset: {asset}\nSignal: {arrow} {direction}\nEntry: {entry}\nExpiry: {expiry} seconds\nAnalysis: {reason}\nID: {id}',
        'plain': '{source} | {asset} | {direction} | {entry} | {expiry}s | {reason}',
        'custom': custom,
    }
    try:
        return formats[template].format_map(fields)
    except (KeyError, ValueError):
        raise ValueError('Invalid template. Allowed: asset, arrow, direction, entry, expiry, reason, source, id')


class Engine:
    def __init__(self, database=':memory:'):
        self.lock = threading.RLock()
        self.db = sqlite3.connect(database, check_same_thread=False)
        self.db.execute('CREATE TABLE IF NOT EXISTS state (id INTEGER PRIMARY KEY, payload TEXT NOT NULL)')
        row = self.db.execute('SELECT payload FROM state WHERE id=1').fetchone()
        previous = json.loads(row[0]) if row else {}
        self.config = {**DEFAULTS, **previous.get('config', {})}
        self.config['auto'] = False
        self.config['telegram'] = False
        self.trades = previous.get('trades', [])
        self.signals = previous.get('signals', [])
        self.logs = previous.get('logs', [])
        self.balance = previous.get('balance', 1000.0)
        for trade in self.trades:
            if trade['status'] == 'OPEN':
                trade['status'] = 'VOID_RESTART'
                self.balance += trade['stake']
        self.running = False
        self.emergency = False
        self.mode = 'disconnected'
        self.markets = []
        self.candles = []
        self.rows = []
        self.cursor = 0
        self.last_tick = 0
        self.feed_time = None
        self.broker_balance = None
        self.analysis = dict(direction='WAIT', reason='Connect a feed or load a replay', votes={}, rsi=None)
        self.last_analyzed = None
        self.session = str(time.time_ns())
        self.outbox = []
        self.log('INFO', 'Started. Execution mode: local paper only. Broker orders are locked.')
        self.save()

    def log(self, level, text):
        self.logs.append(dict(time=time.time(), level=level, text=text))
        self.logs = self.logs[-500:]

    def save(self):
        payload = dict(config=self.config, trades=self.trades, signals=self.signals[-2000:], logs=self.logs, balance=round(self.balance, 8))
        self.db.execute('INSERT OR REPLACE INTO state VALUES (1,?)', (json.dumps(payload, allow_nan=False),))
        self.db.commit()

    def open_trades(self):
        return [t for t in self.trades if t['status'] == 'OPEN']

    def assert_idle(self):
        if self.open_trades():
            raise ValueError('Wait for open paper trades to settle before changing feed/settings')

    def load_replay(self, rows=None, period=None):
        self.assert_idle()
        self.running = False
        self.config['auto'] = False
        self.mode = 'csv' if rows else 'sample'
        if period:
            self.config['timeframe'] = period
        if not rows:
            if not self.config['asset'].startswith('SAMPLE_'):
                self.config['asset'] = 'SAMPLE_EURUSD'
            self.rows = sample(self.config['asset'], self.config['timeframe'])
        else:
            self.config['asset'] = 'IMPORTED_CSV'
            self.rows = rows
        self.config['expiry'] = max(self.config['expiry'], self.config['timeframe'])
        self.config['expiry'] = math.ceil(self.config['expiry']/self.config['timeframe'])*self.config['timeframe']
        self.cursor = 40
        self.candles = self.rows[:self.cursor]
        self.feed_time = self.rows[self.cursor]['time']
        self.last_tick = time.time()
        self.last_analyzed = self.candles[-1]['time']
        self.session = str(time.time_ns())
        self.markets = [dict(id=self.config['asset'], name=self.config['asset'], open=True, payouts={})]
        self.analysis = analyze(self.candles, self.config['strategies'])
        self.log('INFO', 'Loaded ' + self.mode.upper() + ' replay. Prices and P/L are not broker results.')
        self.save()

    def update_config(self, values):
        self.assert_idle()
        if self.running:
            raise ValueError('Stop engine before changing settings')
        c = dict(self.config)
        for k, v in values.items():
            if k in ('stake', 'daily_loss'):
                c[k] = number(v, .01, 10000)
            elif k == 'payout':
                c[k] = number(v, 1, 100)
            elif k in ('max_trades', 'loss_streak'):
                c[k] = int(number(v, 1, 1000))
            elif k in ('timeframe', 'expiry'):
                if int(v) not in (60, 120, 300, 900):
                    raise ValueError('Supported periods: 60, 120, 300, 900')
                c[k] = int(v)
            elif k == 'strategies':
                if not isinstance(v, list) or not v or any(s not in ('trend', 'momentum', 'breakout') for s in v):
                    raise ValueError('Select at least one valid strategy')
                c[k] = list(dict.fromkeys(v))
            elif k in ('auto', 'telegram'):
                if not isinstance(v, bool):
                    raise ValueError('Toggle must be boolean')
                c[k] = v
            elif k == 'asset':
                if not isinstance(v, str) or len(v) > 80:
                    raise ValueError('Invalid asset')
                if self.mode == 'broker' and v not in [m['id'] for m in self.markets]:
                    raise ValueError('Asset is not supplied by this broker')
                c[k] = v
            elif k == 'template':
                if v not in ('compact', 'detailed', 'plain', 'custom'):
                    raise ValueError('Invalid format')
                c[k] = v
            elif k == 'custom':
                if not isinstance(v, str) or len(v) > 2500:
                    raise ValueError('Template too long')
                c[k] = v
            else:
                raise ValueError('Unsupported setting: ' + str(k))
        if self.mode in ('csv', 'sample') and c['expiry'] % c['timeframe']:
            raise ValueError('Replay expiry must be a multiple of candle timeframe')
        dummy = dict(asset='EXAMPLE', direction='CALL', time=0, expiry=60, reason='Preview', source='EXAMPLE', id='preview')
        format_signal(dummy, c['template'], c['custom'])
        changed_feed = (c['asset'], c['timeframe']) != (self.config['asset'], self.config['timeframe'])
        if self.mode == 'csv' and changed_feed:
            raise ValueError('CSV asset/timeframe is fixed by the imported file')
        self.config = c
        if changed_feed:
            self.candles = []
            self.last_tick = 0
            self.last_analyzed = None
            self.analysis = dict(direction='WAIT', reason='Waiting for selected asset data', votes={}, rsi=None)
            if self.mode == 'sample':
                self.load_replay()
        self.log('INFO', 'Settings saved; no broker orders enabled')
        self.save()

    def risk_reason(self, now):
        day = int(now//86400)
        trades = [t for t in self.trades if t['session'] == self.session and int(t['time']//86400) == day and not t['status'].startswith('VOID')]
        pnl = sum(t.get('pnl', 0) for t in trades)
        exposure = sum(t['stake'] for t in self.open_trades())
        if self.emergency:
            return 'Emergency stop active'
        if exposure:
            return 'One open paper trade at a time'
        if self.balance < self.config['stake']:
            return 'Insufficient paper balance'
        if max(0, -pnl) + exposure + self.config['stake'] > self.config['daily_loss']:
            return 'Daily loss budget would be exceeded'
        if len(trades) >= self.config['max_trades']:
            return 'Daily trade count reached'
        losses = 0
        for t in reversed(trades):
            if t['status'] == 'LOSS': losses += 1
            elif t['status'] != 'OPEN': break
        if losses >= self.config['loss_streak']:
            return 'Consecutive-loss limit reached'
        return None

    def order(self, direction, now, price, signal_id=None):
        if self.mode not in ('sample', 'csv'):
            raise ValueError('This release executes replay paper trades only. Broker order submission is locked.')
        if direction not in ('CALL', 'PUT'):
            raise ValueError('Invalid direction')
        reason = self.risk_reason(now)
        if reason:
            raise ValueError(reason)
        if signal_id and any(t.get('signal_id') == signal_id for t in self.trades):
            raise ValueError('Duplicate signal order blocked')
        t = dict(id=str(time.time_ns()), session=self.session, asset=self.config['asset'], direction=direction,
                 time=now, expiry_at=now+self.config['expiry'], entry=price, stake=self.config['stake'],
                 payout=self.config['payout'], status='OPEN', pnl=0, signal_id=signal_id, source=self.mode.upper())
        self.balance -= t['stake']
        self.trades.append(t)
        self.log('INFO', 'Paper order opened: ' + direction)
        return t

    def settle(self, now, price):
        for t in self.open_trades():
            if now >= t['expiry_at']:
                delta = price-t['entry']
                won = delta > 0 if t['direction'] == 'CALL' else delta < 0
                t['status'] = 'DRAW' if delta == 0 else 'WIN' if won else 'LOSS'
                t['exit'] = price
                t['pnl'] = 0 if delta == 0 else round(t['stake']*t['payout']/100, 8) if won else -t['stake']
                self.balance += t['stake']+t['pnl']
                self.log('INFO', f"Paper trade {t['status']}: {t['pnl']:+.2f}")

    def evaluate(self, now, price):
        if not self.candles or self.last_analyzed == self.candles[-1]['time']:
            return
        self.last_analyzed = self.candles[-1]['time']
        self.analysis = analyze(self.candles, self.config['strategies'])
        if self.analysis['direction'] == 'WAIT':
            return
        sig = dict(id=self.session+':'+self.config['asset']+':'+str(self.last_analyzed), asset=self.config['asset'],
                   time=now, candle_time=self.last_analyzed, direction=self.analysis['direction'],
                   reason=self.analysis['reason'], expiry=self.config['expiry'], source=self.mode.upper())
        if any(s['id'] == sig['id'] for s in self.signals): return
        self.signals.append(sig)
        self.signals = self.signals[-2000:]
        self.log('SIGNAL', sig['direction'] + ' ' + sig['asset'])
        if self.config['telegram']:
            self.outbox.append(format_signal(sig, self.config['template'], self.config['custom']))
        if self.config['auto']:
            try:
                self.order(sig['direction'], now, price, sig['id'])
            except ValueError as e:
                self.log('SKIP', str(e))

    def step(self):
        if self.mode not in ('sample', 'csv') or self.cursor >= len(self.rows)-1:
            self.running = False
            return
        closed = self.rows[self.cursor]
        self.cursor += 1
        next_c = self.rows[self.cursor]
        self.candles.append(closed)
        self.candles = self.candles[-250:]
        self.feed_time = next_c['time']
        self.last_tick = time.time()
        self.settle(self.feed_time, next_c['open'])
        self.evaluate(self.feed_time, next_c['open'])
        if self.cursor >= len(self.rows)-1:
            self.running = False
            for t in self.open_trades():
                t['status'] = 'VOID_END_OF_DATA'
                self.balance += t['stake']
            self.log('INFO', 'End of replay; incomplete trades voided and stakes refunded')
        self.save()

    def manual_order(self, direction):
        if not self.rows or self.cursor >= len(self.rows)-1:
            raise ValueError('Load a replay first')
        row = self.rows[self.cursor]
        t = self.order(direction, row['time'], row['open'])
        self.save()
        return t

    def snapshot(self):
        closed = [t for t in self.trades if t['status'] in ('WIN', 'LOSS', 'DRAW')]
        wins = sum(t['status'] == 'WIN' for t in closed)
        equity, peak, drawdown = 1000, 1000, 0
        for t in closed:
            equity += t['pnl']; peak=max(peak,equity); drawdown=max(drawdown,peak-equity)
        return dict(config=self.config, mode=self.mode, running=self.running, emergency=self.emergency,
                    markets=self.markets, candles=self.candles, feed_time=self.feed_time,
                    age=None if not self.last_tick else round(time.time()-self.last_tick, 1), broker_balance=self.broker_balance,
                    balance=round(self.balance, 2), analysis=self.analysis, signals=self.signals[-100:],
                    trades=self.trades[-300:], logs=self.logs[-100:], cursor=self.cursor, total=len(self.rows),
                    stats=dict(closed=len(closed), wins=wins, win_rate=round(100*wins/len(closed), 1) if closed else None,
                               pnl=round(sum(t['pnl'] for t in closed), 2), drawdown=round(drawdown, 2)))


def backtest(rows, period, config):
    e = Engine()
    e.config.update(config)
    e.config['timeframe'] = period
    e.load_replay(rows, period)
    e.config['auto'] = True
    while e.cursor < len(e.rows)-1:
        e.step()
    result = e.snapshot()['stats']
    result['trades'] = e.trades
    result['note'] = 'Historical replay, next-candle-open entry, fixed assumed payout, no broker fills or latency model. Not out-of-sample unless your CSV is held out.'
    e.db.close()
    return result
