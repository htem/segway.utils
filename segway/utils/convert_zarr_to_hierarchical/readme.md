This script converts a "flat" zarr file (`.` separators) to a hierarchical zarr format (`/` separator). This is useful/required for big datasets to avoid placing many millions of files in the same folder which can cause problems with many file systems.

## TODO
- Re-write code to be an actual function.
