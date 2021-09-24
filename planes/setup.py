from setuptools import setup, find_packages

setup(
    name='plane_crash',
    version='0.1.0',
    packages=find_packages(include=['plane_crash', 'plane_crash.*']),
    setup_requires=['pytest-runner', 'flake8'],
    tests_require=['pytest'],
)