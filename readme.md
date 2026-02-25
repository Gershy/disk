# TODO

Looks like someone needs to write this readme...

## Terminology

- "Lore" - a tradition leading to a set of knowledge/facts. Correlates to data storage engines,
  e.g. filesystems vs databases vs api storage etc.
- "Scholar" - one who studies lore; isolation is implemented at the level of scholars (what one
  scholar produces will not conflict with what another scholar learns)
- "Fact" - something known with a Lore; correlates to the individual nodes/items/records in a
  particular storage engine - e.g. items in postgres or directories/files in a filesystem

## Tech debt

- This repo is called "disk", but houses both persistence-layer-agnostic code (`Lore`) - should
  probably move agnostic code to @gershy/lore, and rename this repo to @gershy/lore-disk
- Need to think about Writable backpressure (for head streams)
- Typing around head stream is a little shaky - e.g. "Writable" conflicts with node.js Writable