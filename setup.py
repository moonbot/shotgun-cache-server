#!/usr/bin/env python
# -*- coding: utf-8 -*-

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

import versioneer

versioneer.VCS = 'git'
versioneer.versionfile_source = 'shotgunCache/_version.py'
versioneer.versionfile_build = 'shotgunCache/_version.py'
versioneer.tag_prefix = ''  # tags are like 1.2.0
versioneer.parentdir_prefix = 'shotgunCache'  # dirname like 'myproject-1.2.0'

readme = open('README.md').read().strip()
license = open('LICENSE').read().strip()

setup(
    name='shotgunCache',
    version=versioneer.get_version(),
    license=license,
    cmdclass=versioneer.get_cmdclass(),
    description='Shotgun Cache Server',
    long_description=readme,
    author='Moonbot Studios',
    author_email='brennan@moonbotstudios.com',
    url='https://github.com/moonbot/shotgun-cache-server',
    packages=[
        'shotgunCache',
    ],
    scripts=[
        'bin/shotgunCache'
    ],
    package_dir={'shotgunCache':
                 'shotgunCache'},
    include_package_data=True,
    install_requires=[
        'elasticsearch>=1.4.0',
        'pyyaml>=3.11',
        'ruamel.yaml>=0.8',
        'pyzmq>=13.1.0',
        'shotgun_api3',
    ],
    zip_safe=False,
    keywords='shotgunCache',
)
