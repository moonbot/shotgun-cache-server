#!/usr/bin/env python
# -*- coding: utf-8 -*-


try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

import versioneer

versioneer.VCS = 'git'
versioneer.versionfile_source = 'shotgunCacheServer/_version.py'
versioneer.versionfile_build = 'shotgunCacheServer/_version.py'
versioneer.tag_prefix = ''  # tags are like 1.2.0
versioneer.parentdir_prefix = 'shotgunCacheServer'  # dirname like 'myproject-1.2.0'

readme = open('README.md').read()

requirements = [
    # TODO: put package requirements here
]

test_requirements = [
    # TODO: put package test requirements here
]

setup(
    name='shotgunCacheServer',
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    description='Shotgun Cache Server',
    long_description=readme + '\n\n',
    author='Moonbot Studios',
    author_email='brennan@moonbotstudios.com',
    url='https://git/tools/shotgunCacheServer',
    packages=[
        'shotgunCacheServer',
    ],
    package_dir={'shotgunCacheServer':
                 'shotgunCacheServer'},
    include_package_data=True,
    install_requires=requirements,
    zip_safe=False,
    keywords='shotgunCacheServer',
    classifiers=[
        'Intended Audience :: Developers',
        'Natural Language :: English',
        "Programming Language :: Python :: 2",
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
    ],
    test_suite='tests',
    tests_require=test_requirements
)
