from setuptools import setup, find_packages
import glob
import os
import sys

from taca import __version__

try:
    with open("requirements.txt", "r") as f:
        install_requires = [x.strip() for x in f.readlines()]
except IOError:
    install_requires = []

try:
    with open("dependency_links.txt", "r") as f:
        dependency_links = [x.strip() for x in f.readlines()]
except IOError:
    dependency_links = []


setup(name='taca',
    version=__version__,
    description="Tool for the Automation of Cleanup and Analyses",
    long_description='This package contains a set of functionalities that are '
                   'useful in the day-to-day tasks of bioinformatitians in '
                   'National Genomics Infrastructure in Stockholm, Sweden.',
    keywords='bioinformatics',
    author='Guillermo Carrasco',
    author_email='guille.ch.88@gmail.com',
    url='http://taca.readthedocs.org/en/latest/',
    license='MIT',
    packages=find_packages(exclude=['ez_setup', 'examples', 'tests']),
    scripts=glob.glob('scripts/*.py'),
    include_package_data=True,
    zip_safe=False,
    entry_points={
        'console_scripts': ['taca = taca.cli:cli'],
        'taca.subcommands': [
            'storage = taca.storage.cli:storage',
            'analysis = taca.analysis.cli:analysis',
            'bioinfo_deliveries = taca.utils.cli:bioinfo_deliveries',
            'server_status = taca.server_status.cli:server_status',
        ]
    },
    install_requires=install_requires,
    dependency_links=dependency_links
)
