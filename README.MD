# PoreMongo

PoreMongo is a scalable re-imagination of Nick Loman's excellent [`poredb`](https://github.com/nickloman/poredb) for singal-level data management in MongoDB. It allows the user to connect to a MongoDB server via URI. At this stage, the module is primarily designed to track `Fast5` files and signal-level read information, without storing the actual signal arrays. Users can tag and comment on file collections for custom data selections, spanning sequencing runs, flowcells or experiments. Files can be efficiently selected, sampled and filtered in the database via simplified methods accessible from instances of `PoreMongo`. Computationally expensive operations (indexing or copying files, extracting signal arrays) are therefore decoupled from the data selection process in PoreMongo.

PoreMongo is under development, stay tuned!

