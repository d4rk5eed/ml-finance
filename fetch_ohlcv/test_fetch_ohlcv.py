import unittest
import subprocess
import os
import pandas as pd

class TestFetchOHLCV(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Тестовая выгрузка небольшого набора данных
        cmd = [
            'python', 'fetch_ohlcv.py',
            '-N', '10',
            '-T', 'BTC/USDT',
            '-I', '5m',
            '-o', 'test_data.csv'
        ]
        subprocess.run(cmd, check=True, timeout=5000)

    def test_file_creation(self):
        self.assertTrue(os.path.exists('test_data.csv'))

    def test_data_integrity(self):
        df = pd.read_csv('test_data.csv')
        self.assertEqual(len(df), 10)
        self.assertListEqual(
            list(df.columns),
            ['timestamp', 'open', 'high', 'low', 'close', 'volume', 'datetime']
        )

    def test_cli_arguments(self):
        # Тест аргументов по умолчанию
        result = subprocess.run(
            ['python', 'fetch_ohlcv.py'],
            capture_output=True,
            text=True
        )
        self.assertIn('BTC/USDT', result.stdout)
        self.assertIn('5m', result.stdout)

    @classmethod
    def tearDownClass(cls):
        os.remove('test_data.csv')

if __name__ == '__main__':
    unittest.main()
