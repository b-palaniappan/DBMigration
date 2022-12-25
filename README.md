# DBMigration
Migrate SQL DB including Structure, relationship and data from one DB to Other DB.

## Configuration Steps
* Add Source and Destination DB connection information in `src/main/resources/application.properties` file.
* If there are any domain tables (which are static domain data for which the data need to be copied with same IDs), that need to be configured in `src/main/resources/domainTableList.txt`.
* If only few tables need to be transferred, use `src/main/resources/tables.txt` file.
