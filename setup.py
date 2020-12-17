from setuptools import setup

setup(
    name='bibliograph',
    url='https://github.com/shortda/bibliograph',
    author='Devin Short',
    author_email='short.devin@gmail.com',
    packages=['bibliograph'],
    install_requires=['ads', 'datetime', 'networkx', 'pandas', 'progressbar'],
    version='0.01.0-alpha',
    license='MIT',
    description='A Python package for visualizing and analyzing bibliographic data',
    long_description=open('README.md').read()
)