import unittest
import shardcacheclient

class Test(unittest.TestCase):
    def runTest(self):
        x = shardcacheclient.Client((('peer1', 'localhost:4444'),), '')
        x.set('mannaia', '234')
        x.get('mannaia')

if __name__ == '__main__':
    unittest.main()

