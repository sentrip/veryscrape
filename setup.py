#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""The setup script."""

from setuptools import setup, find_packages

with open('README.rst') as readme_file:
    readme = readme_file.read()

with open('HISTORY.rst') as history_file:
    history = history_file.read()

requirements = [
    'Click>=6.0', 'aiohttp>=2.3.10', 'lxml>=3.5.0', 'async_timeout==2.0.0',
    'aioauth_client>=0.8.0', 'fake_useragent>=0.1.10', 'newspaper3k>=0.2.6',
    'twingly_search>=2.1.1', 'deprecation>=2.0.2', 'proxybroker>=0.3.1',
    'redis>=2.10.6'
]

setup_requirements = ['pytest-runner', ]

test_requirements = ['pytest', ]

setup(
    author="Djordje Pepic",
    author_email='djordje.m.pepic@gmail.com',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: GNU General Public License v3 (GPLv3)',
        'Natural Language :: English',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
    ],
    description="Simple time-series streaming of social media data",
    entry_points={
        'console_scripts': [
            'veryscrape=veryscrape.cli:main',
        ],
    },
    install_requires=requirements,
    license="GNU General Public License v3",
    long_description=readme + '\n\n' + history,
    include_package_data=True,
    keywords='veryscrape',
    name='veryscrape',
    packages=find_packages(include=['veryscrape', 'veryscrape.scrapers']),
    setup_requires=setup_requirements,
    test_suite='tests',
    tests_require=test_requirements,
    url='https://github.com/sentrip/veryscrape',
    version='0.1.3',
    zip_safe=False,
)
