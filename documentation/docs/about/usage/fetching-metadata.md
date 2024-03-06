---
sidebar_position: 3
---

# Fetching metadata

Two client methods have been created to fetch basic metadata for one or more tags.

### get_units()

Fetches the engineering unit(s) for the tag(s) provided. The argument `tags` can be either a single tagname as string,
or a list of tagnames.

### get_description()

Fetches the description(s) for the tag(s) provided. The argument `tags` can be either a single tagname as string,
or a list of tagnames.

**Example**:

``` python
tags = ["BA:ACTIVE.1", "BA:LEVEL.1", "BA:CONC.1"]
units = c.get_units(tags)
desc = c.get_descriptions(tags)
tag = "BA:CONC.1"
df[tag].plot(grid=True, title=desc[tag]).set_ylabel(units[tag])
```

