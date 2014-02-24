from distutils.core import setup, Extension, Command

class DiscoverTest(Command):
    user_options = []

    def initialize_options(self):
            pass

    def finalize_options(self):
        pass

    def run(self):
        import os
        import sys
        import unittest

        # get setup.py directory
        setup_file = sys.modules['__main__'].__file__
        setup_dir = os.path.abspath(os.path.dirname(setup_file))
        setup_dir = os.path.join(setup_dir, 'tests')

        # use the default shared TestLoader instance
        test_loader = unittest.defaultTestLoader

        # use the basic test runner that outputs to sys.stderr
        test_runner = unittest.TextTestRunner()

        # automatically discover all tests
        # NOTE: only works for python 2.7 and later
        test_suite = test_loader.discover(setup_dir)

        # run the test suite
        test_runner.run(test_suite)


module1 = Extension('shardcacheclient',
                    sources = ['src/client.c'],
                    libraries=['shardcache'])

setup (name = 'ShardcacheClient',
       version = '1.0',
       description = 'Bindings to the shardcache-client library.',
       ext_modules = [module1],
       cmdclass = {'test': DiscoverTest})

