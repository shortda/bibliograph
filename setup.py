from setuptools import setup

setup(
    name='bibliograph',
    url='https://github.com/shortda/bibliograph',
    author='Devin Short',
    author_email='short.devin@gmail.com',
    packages=['bibliograph'],
    install_requires=['ads', 'csv', 'datetime', 'json', 'networkx', 'os', 'pandas', 'progressbar', 'shutil'],
    version='0.01',
    license='MIT',
    description='A Python package for visualizing and analyzing bibliographic data',
    long_description=open('README.md').read()
)