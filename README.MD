# Poremongo <a href='https://github.com/esteinig'><img src='docs/img/poremongo.png' align="right" height="210" /></a>

![](https://img.shields.io/badge/version-0.3a-yellow.svg)
![](https://img.shields.io/badge/docs-repo-green.svg)
![](https://img.shields.io/badge/lifecycle-maturing-blue.svg)

### Overview
---

Interface for nanopore signal read management in MongoDB.


### Install
---

With MongoDB:

```
conda install -c bioconda poremongo
```

From PyPI without MongoDB:

```
pip install poremongo
```

### Example: Python API
---

```python
from poremongo import PoreMongo

pm = PoreMongo(uri="mongodb://user:pwd@server:port/database")
pm.connect()

pm.index("path/to/fast5", batch_size=500, ncpu=4)
pm.tag(path_query="path/to/fast5", tags=("TB", "R9.4", "RUNID_1726"))

pm.display("tags")

fast5 = pm.query(tag_query="TB")

for f5 in fast5:
  reads = f5.get_reads(template=True)
  print(f5.path, f5.tags, reads)
  
fast5_sampled = pm.sample(fast5, tags="TB", limit=10, proportion=None)

pm.copy(fast5_sampled, outdir="tmp", ncpu=1)

pm.disconnect()

```

PoreMongo is under construction.
