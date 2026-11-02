import os
import tempfile
import unittest
from core import Engine, analyze, backtest, parse_csv, sample
from broker import normalize_candles

class EngineTests(unittest.TestCase):
    def setUp(self): self.e=Engine();self.e.load_replay()
    def tearDown(self): self.e.db.close()
    def test_win_loss_accounting(self):
        e=self.e;e.order('CALL',100,10);e.settle(220,11)
        self.assertAlmostEqual(e.balance,1000.8)
        e.order('PUT',300,10);e.settle(420,11)
        self.assertAlmostEqual(e.balance,999.8)
    def test_draw_refunds(self):
        self.e.order('CALL',100,10);self.e.settle(220,10)
        self.assertEqual(self.e.balance,1000)
    def test_duplicate_signal(self):
        e=self.e;e.order('CALL',100,10,'s');e.settle(220,11)
        with self.assertRaisesRegex(ValueError,'Duplicate'):e.order('CALL',300,10,'s')
    def test_loss_cap_includes_new_stake(self):
        e=self.e;e.config['daily_loss']=1.5;e.order('CALL',100,10);e.settle(220,9)
        with self.assertRaisesRegex(ValueError,'loss budget'):e.order('CALL',300,10)
    def test_emergency(self):
        self.e.emergency=True
        with self.assertRaises(ValueError):self.e.manual_order('CALL')
    def test_broker_order_blocked(self):
        self.e.mode='broker'
        with self.assertRaisesRegex(ValueError,'locked'):self.e.order('CALL',100,10)
    def test_nonfinite_and_invalid_strategy(self):
        for value in (float('nan'),float('inf'),-1):
            with self.assertRaises(ValueError):self.e.update_config({'stake':value})
        with self.assertRaises(ValueError):self.e.update_config({'strategies':['magic']})
    def test_no_mutation_of_old_signals(self):
        e=self.e
        for _ in range(50):e.step()
        old=[dict(s) for s in e.signals]
        for _ in range(50):e.step()
        self.assertEqual(e.signals[:len(old)],old)
    def test_risk_config_blocked_with_open_trade(self):
        self.e.manual_order('CALL')
        with self.assertRaises(ValueError):self.e.update_config({'stake':2})
    def test_restart_voids_open_position(self):
        with tempfile.TemporaryDirectory() as d:
            path=os.path.join(d,'state.db');a=Engine(path);a.load_replay();a.manual_order('CALL');a.db.close()
            b=Engine(path);self.assertEqual(b.balance,1000);self.assertFalse(b.config['auto']);self.assertEqual(b.trades[-1]['status'],'VOID_RESTART');b.db.close()
    def test_csv_rejects_duplicate(self):
        header='time,open,high,low,close\n';text=header+''.join('1767225600,1,2,0.5,1.5\n' for _ in range(40))
        with self.assertRaisesRegex(ValueError,'increasing'):parse_csv(text)
    def test_candle_validation(self):
        with self.assertRaises(ValueError):normalize_candles([dict(time=100,open=2,high=1,low=.5,close=2)],60)
    def test_backtest_is_deterministic_and_no_live_orders(self):
        rows=sample('SAMPLE_EURUSD',60,180)
        a=backtest(rows,60,self.e.config);b=backtest(rows,60,self.e.config)
        self.assertEqual(a['pnl'],b['pnl']);self.assertEqual(a['closed'],b['closed'])
        self.assertTrue(all(t['source']=='CSV' for t in a['trades']))
    def test_prefix_causality(self):
        rows=sample('x',60,100);a=analyze(rows[:50],['trend','momentum'])
        rows[80]['close']=9999
        self.assertEqual(a,analyze(rows[:50],['trend','momentum']))
    def test_entry_is_next_open(self):
        e=self.e;e.config['strategies']=['trend'];e.config['auto']=True
        for _ in range(100):
            before=len(e.trades);e.step()
            if len(e.trades)>before:
                self.assertEqual(e.trades[-1]['entry'],e.rows[e.cursor]['open']);return
        self.fail('Expected one signal in deterministic sample')

if __name__=='__main__':unittest.main()
