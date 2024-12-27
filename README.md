# WAL

## Backstory
A few months before, I started to dig deeper into the world of databases. One day I thought that it'd be nice to code something up, as theory without pratice doesn't make that much sense. So I decided to first write my own WAL implementation. 

## What it can do
Currently, it's just a primitive WAL that writes data to internal buffer and flushes it to persistent storage every 10s (will make it configurable later on). It doesn't write everything to a single file, instead it splits everything up into segments of a fixed size (already a configurable parameter). And to sum it up, it also supports replaying, because WAL is not WAL without capability to replay data. 
