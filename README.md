# sodata
Processing scripts for StackOverflow data dump

## Installation

```bash
# preferably in a virtualenv
pip install git+https://github.com/isofew/sodata
```

## Usage

Download the 7z data dumps starting with "stackoverflow.com-" from [here](https://archive.org/details/stackexchange).

Then, extract all xml files into a directory at `/path/to/data` and use the following code to prepare the data

```python
import sodata.scripts

sodata.scripts.xml2json('/path/to/data')
sodata.scripts.join_puv('/path/to/data')
```

Functions unique to individual studies are under the `sodata.studies` package.
