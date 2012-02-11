#!/usr/bin/env python

from distutils.core import setup

try:
    from distutils.command.build_py import build_py_2to3 as build_py
except ImportError:
    from distutils.command.build_py import build_py

setup(
    name = 'pymapreduce',
    version = '0.1',
    url = 'https://github.com/fdouetteau/PyMapReduce',
    description = 'Simple Mapreduce in Python',
    author = 'Florian Douetteau',
    author_email = 'florian@douetteau.net',
    license = 'MIT',
    platforms = 'any',
    py_modules = [
        'pymapreduce'
    ],
    requires = [
    ],
    classifiers = [
        'Environment :: Web Environment',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Topic :: Internet :: WWW/HTTP :: Dynamic Content',
        'Topic :: Software Development :: Libraries :: Python Modules'
    ],
    cmdclass = {'build_py': build_py}
)
