# PoreMongo

PoreMongo is a re-imagination of Nick Loman's [`poredb`](https://github.com/nickloman/poredb) for signal-level data management in MongoDB. It allows the user to connect to a MongoDB server via URI. At this stage, the module is primarily designed to track `Fast5` files and signal-level read information, without storing the actual signal arrays. Users can tag and comment on file collections for custom data selections, spanning sequencing runs, flowcells or experiments. Files can be efficiently selected, sampled and filtered in the database via simplified methods accessible from instances of `PoreMongo`. Computationally expensive operations (indexing or copying files, extracting signal arrays) are therefore decoupled from the data selection process in PoreMongo.

PoreMongo is under development, stay tuned!

### Usage (Development Version)

Create an instance of `PoreMongo` by passing your MongoDB connection string (URI). You can learn more about the format of the connection string from the [reference manual for MongoDB](https://docs.mongodb.com/manual/reference/connection-string/).

Usually the format is something like this:

```python
pongo = PoreMongo(uri="mongodb://user_name:secret_password@server_ip:server_port/database_name")
```
You can now inititate the connection:

```python
pongo.connect()
```

The first step is to index your `Fast5` files from a completed sequencing run in their respective directory. This is achieved with the `index` method of `PoreMongo`.

Sequencing runs can generate a large number of files, so the initial indexing will take some time, especially when specifying a recursive walk through the specified directory. This is often necessary due to the `pass/fail` and enumerated sequence run directory structure forced by ONT. You can speed up the process by either indexing the specific directory containing the `Fast5` files or by specifying the number of processors for parallel indexing.

```python
pongo.disconnect()
```
