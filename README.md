RPKI Wayback Machine 2.0
========================

*Naming suggestions welcome*

The design
----------
In principle, the program consists of a startup phase, where it downloads the Routinator log from ftp.ripe.net. It parses the RRDP repositories from that log, and adds them to a set. For every item in that set, a timer that runs every 1 minute (configurable) is started, which looks at the notification XML, and downloads the deltas between the last time it visited and the most recent version. If that is not possible (e.g. not all deltas are available anymore) it will download the snapshot. This means all RRDP repositories run in principle in parallel.

The result is written to a SQlite database. The main idea of this database is that you can request the state of the RPKI at any random point in time and recreate it locally in a form that can be parsed by a relying party client (e.g. Routinator).

To generate the .jar, run:
```
mvn clean package
```

Reconstruction
--------------
In order to reconstruct the RPKI at a specific point in time, look at `reconstruct.py`.


TODO
----
- Switch to a local Routinator instance
- Add support for rsync

