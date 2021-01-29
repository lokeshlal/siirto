### Siirto

Migrate data (CDC) from database to other sinks

##### Structure

```
siirto
|
|___database_operators
|   |___(base_database_operator) BaseDataBaseOperator
|   |___(postgres_operator) PostgresOperator
|   
|___plguins
    |
    |___cdc
    |   |___(cdc_base) CDCBase
    |   |___(pg_default_cdc_plugin) PgDefaultCDCPlugin
    |   
    |___full_load
        |___(full_load_base) FullLoadBase
        |___(pg_default_full_load_plugin) PgDefaultFullLoadPlugin
```

Database operator classes defines the functionality/flow of calling full load plugin and/or cdc plugin.

Each operator class calls the separate plugins to process. CDC plugins are suppose to run as a single process for all the tables. However, full load plugins will run one process for one table export.

As of now, Postgres operator is only implemented and copies the data on NFS. However, as per requirements this can be enhanced to move to kafka or directly to other data sinks.

##### Configuration

Configuration file is available at: ./siirto/configuration.cfg  

##### Code documentation

To update the documentation run `python generate_doc.py`. Code generate the documentation in folder `./doc` folder

Documentation is available at https://lokeshlal.github.io/siirto/siirto.html
    
