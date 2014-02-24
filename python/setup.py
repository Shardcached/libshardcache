from distutils.core import setup, Extension

module1 = Extension('shardcacheclient',
                    sources = ['src/client.c'],
                    libraries=['shardcache'])

setup (name = 'ShardcacheClient',
       version = '1.0',
       description = 'Bindings to the shardcache-client library.',
       ext_modules = [module1])

