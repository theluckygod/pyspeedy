# pyspeedy
Welcome to the Python Speedy Utilities Collection, a curated assembly of Python libraries designed to supercharge your coding workflow. This repository is dedicated to Python developers looking for ways to accelerate their development process, with a focus on efficiency, performance, and convenience. This repository only support Python 3.7+.

## Table of Contents
- [Introduction](#introduction)
- [Installation](#installation)

## Introduction
The key features are:

### File utils 
```python
from pyspeedy.files import *
# load all csv files in the data directory and merge them into a single dataframe
# currently, support for csv, json, txt
df = load_by_ext("data/*.csv", try_to_merge=True) 
# write the dataframe to a csv file with a date tag
write_by_ext(df, "outputs/merged.csv", to_add_date_tag=True)
# write to "outputs/merged_v24.04.01.csv"
```

### Text utils
```python
from pyspeedy.text import *
teen_code_decode("a k còn yêu e nữa phải k")
#> "anh không còn yêu em nữa phải không"
```

### VSCode utils
```bash
# dump the well-configured vscode settings to current directory
python -c "from pyspeedy.vscode import *; dump_settings()"
```

### Pytest utils
```bash
# dump the template of pytest to current directory
python -c "from pyspeedy.pytest import *; dump_template()"
```

### Design Patterns
```python
from pyspeedy.patterns import SingletonMeta

class MySingleton(metaclass=SingletonMeta):
    value: str = None
    def __init__(self, value: str) -> None:
        self.value = value
    def some_business_logic(self):
        pass
```

## Installation
```bash
pip install -e git+https://github.com/theluckygod/pyspeedy.git#egg=pyspeedy
```

