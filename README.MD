# PoreMongo

Please see Jupyter notebooks for documentation.

### Installation
---

```
pip install poremongo
```

### Quickstart
---

```python
from poremongo import PoreMongo

pm = PoreMongo(uri="mongodb://user:pwd@server:port/database")
pm.connect()

pm.index("path/to/fast5", batch_size=100, ncpu=1)
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
