# bibliograph
### A Python package for visualizing and analyzing bibliographic data

**bibliograph** takes a BibTex file and provides a flexible representation of a citation network using pandas DataFrames and NetworkX graphs. This is an extremely early version of a package under development as part of a dissertation in the history of science. The first alpha version is installable via 

    pip install git+https://github.com/shortda/bibliograph.git@v0.01.01-alpha01 

## Features

* Quick access to indexed bibliographic data via the NASA Astrophysics Data System API.
* Multiple methods to store and retrieve citation data from local files, enabling fast manual entry of citation data that is not indexed.
* Citation graph implemented in NetworkX, providing powerful network analysis and easy export for multiple visualization tools.
