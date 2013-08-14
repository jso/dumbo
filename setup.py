from setuptools import setup, find_packages

setup(
    name = 'dumbo',
    version = '0.21.36',
    author = 'Klaas Bosteels',
    author_email = 'klaas@last.fm',
    license = 'Apache Software License (ASF)',
    packages = find_packages(),
    entry_points = {
        'console_scripts': [
            'dumbo = dumbo:execute_and_exit',
        ]
    },
    zip_safe = True,
    install_requires = ['typedbytes'],
    test_suite = 'nose.collector',
    tests_require = ['nose']
)
