# 1.3.0

* Adding eager loading of queries. [#145](https://github.com/C2FO/patio/pull/145)

# 1.2.0

* Fix where text types would be cast to null when empty. [#146](https://github.com/C2FO/patio/pull/146)

# 1.1.1

* Adding `patio.parseInt8` and `patio.defaultPrimaryKeyType` options.

# 1.1.0

* Adding patio error event on connection errors.

# 1.0.0

* Adding Node 4 and 5 support.

# 0.9.5

* Remove `id` key from being created in a JsonArray
* Add table inheritance for Postgres


# 0.9.4

* Add [coveralls.io](https://coveralls.io/r/C2FO/patio?branch=master)


# 0.9.3

* Allow for `literal` in dataset `from` clause

# 0.9.2

* Updated `comb` module

# 0.9.1

* Updated `pg` module
* Fixed issue where connection would hang in postgres adapter

# 0.9.0

* Fixed issue with `time` db type in postgres adapter
* Added grunt
* Updated dependencies for mysql and postgres
* Added support for setting `batchSize` and `highWaterMark` when using the `stream` dataset method

# 0.8.1

* Added Array support for JSON datatypes.


# 0.8.0

* Removed `\` escaping in postgres adapter as it is not the default in versions >= 9.1

#0.7.0

* Fix for issue [#121](https://github.com/C2FO/patio/issues/121) added the table name to the error thrown.
* Merged [#120](https://github.com/C2FO/patio/pull/120) this allows tables registered with DB to be looked up properly.
   * This will break any `getModel` call where a table with the same name is added twice.
* Added documentation about running tests.

#0.6.1

* Added details for logging if the err.detail exists.
* Changed streaming highWaterMark

#0.6.0

* Fixed issue where grouped expressions with arrays and hashes as items, the expressions generated from the hashes are anded and each array item is ORed properly [#115](https://github.com/C2FO/patio/pull/115)

#0.5.4

* Updated to `errback` the query promise when an error is caught.
* Fixed issue with setting the `handleRowDescription` on a patio query.

#0.5.3

* Fixed issue with `logError` in transactions.

#0.5.2

* Fixed issue with `streams` in transactions.

#0.5.1

* Update `comb` to `v0.3.0`
* Fixed issues with `savepoints` and update transaction to conform with docs. [#110](https://github.com/C2FO/patio/pull/110)

#0.5.0

* Added a new `stream` method to allow the streaming of large datasets from the database.
* Added a new `isolate` option to `db.transaction` to ensure that the transaction is not nested inside of a concurrently running transaction.
* Added more indepth error handling to connections on all database adapters.
* Refactored `fetchRows`, and `execute` to be common between adapters.
* Updated Readme with better examples.


# 0.4.1

* Added error handling on connection errors and reconnects if a connection errors

# 0.4.0

* Added support for redshift database.

# v0.3.1

* Normalized thrown errors.

# v0.3.0

* Added more in depth support for the `JSON` datatype.
  * Databases now natively support the JSON datatype.
  * Values are type casted to a JSON datatype when used with a model.


# v0.2.18

* Added new `patio.sql.json` type for storing JSON datatypes.

# v0.2.17

* Changed to use use `pg.js`
* Removed the storing of columns in datasets

# v0.2.16

* Fixed constraint creation to accept a function when creating or altering constraints.

#v0.2.15

* Updated patio migrate to use an exit code of `1` if the migration fails. [#92](https://github.com/C2FO/patio/issues/92)
* Fixed the use of hashes in `andGrouped*` methods.

# v0.2.14

* Converted uses of `.then` to `.chain`
* Refactored code to not use `.bind` or `hitch` for performance.
   * Test ran in `~15sec` before now `~10sec`
* Overall performace increases

# v0.2.13

* Updated `multiInsert` to support the returning of results.
* Updated the postgres adapter to not change strings with double `_` to identifiers.

# v0.2.12

* Patio migrate missingArgument undefined [#89](https://github.com/C2FO/patio/pull/89)
* Using stirngToIdentifier on hash keys in `select()` for doing aliases [#87](https://github.com/C2FO/patio/pull/87)

* v0.2.11

* Updated 'grouped' methods to handle cases where they are called without an existing clause on the DS, and regenerated docs.

# v0.2.9

* Updated model to store transformed values in the changed hash
* Update docs for pr [#82](https://github.com/C2FO/patio/pull/82)

# 0.2.8

* Updated listener to strip generated quotes.


# 0.2.7

* Fixed issue with quoted channel names

# 0.2.6

* Updated to use postgres native drivers unless they are not found.

# 0.2.5

* Added support for postgres listen/notify.

# 0.2.4/ 2013-10-23

* Changed to prevent errors that occur in a transaction from being wrapped in a new error and losing the stack.


# 0.2.3 / 2013-10-22

* Changed drop view code to ensure that views are dropped in order.

# 0.2.2 / 2013-10-21

* Added support for postgres 9.3 materialized views.
   * `createMaterializedView`: Create a new materialized view.
   * `dropMaterializedView`: Drops a materialized view.
   * `refreshMaterializedView`: Refresh a materialized view.

# 0.2.1 / 2013-10-16

* Fixed issue in connection pool where a connection would never be returned to the pool.

# 0.2.0 / 2013-09-30

* Upgraded `pg`, `mysql`, and `validator`.
* Added a v0.10 build to travis
* Merged issue [#68](https://github.com/C2FO/patio/pull/68).

# 0.1.7 / 2012-12-4

* Fixed issue where port was not added to connection options.


# 0.1.6 / 2012-11-16

* Upgraded comb to v0.1.10
* Fixed issue where eager many to one returns a promise if null (@mbenedettini)


# 0.1.5 / 2012-09-20

* Fixed issue with sql.literal not accepting a single identifier
* Fixed Issue with one to many model if the table is plurarlized
* Fixed issue with Dataset#group not using stringToIdentifier to convert column arguments to identifiers
* Cleaned up model code to use async array and chain methods so errors are propagated properly.
* updated timestamp plugin to set updated/created when just retreving sql
* Fixed primary key caching issue

# 0.1.4 / 2012-09-20

* added a rowCB for custom dataset model
* added insert, update and remove sql properties on models
* re-added jscoverage submodule


# 0.1.3 / 2012-09-10

* Added travis CI
* Code clean up
* Updated comb version

# 0.1.2 / 2012-09-06

* Updated for new comb api.
* Fixed tests
* Update db.tranaction to require a promise or the callback function to be called.

# 0.1.1 / 2012-08-29

* new patio.Dataset features
  * sourceList - get all sources as identifiers
  * joinSourceList - get all join sources
  * hasSelectSource - returns true if there are not any select sources (i.e select *)
  * seleIfNoSource - add the selects if there is not currently a select
* Fixed issue with patio.Model#_setFromDb where values not in the models table columns would be in accesible, (i.e a join with a model would not show the join columns)
* updated docs

# 0.1.0 / 2012-08-25

* Added custom getters (mbenedettini)
* Added Validator plugin for models
* Change model inheritance configure method to return this for chaining
* Updated all promise returning methods to return the `.promise()`
* Updated docs new docs layout
* Updated docs
  * Updated logging.md to use new comb logging API
  * updated associations.md to use chaning api
  * Added examples to model.md for new getter functionality and added a setter and getter example
  * Added Model Validation page and added validation to plugins page.
  * Updated index.html page
* New Tests for both custom getters and setters
* Changed tests to run both mysql and postgres
* Bug fixes
  * Fixed bug where custom datasets for model definition would not work
  * Changed the storage of model classes so patio.getModel will always work even with datasets.


# 0.0.9 / 2012-08-16

* Fixed bug where default that are buffers were not handle properly (#35)
* Fixed bug with inheritance loading
* Updated benchmark
* Changed `oneToMany` to load models when removing to call hooks
* New ColumnMapper plugin
* added alwaysQualify which will qualify a dataset every time sql is generated from it.
* converted model to use comb.serial instead of the promise chaining api
* changed reload of save and update to be invoked after post save and update have been called.
* Merged pull request from @mbenedettini to lazy initialize hive

# 0.0.8 / 2012-07-13

* Updated Docs now using [coddoc](http://github.com/doug-martin/coddoc) for doc generation
  * Documented Plugins
  * Updated docs for new Model definitions
* Migrated tests to use [it](http://github.com/doug-martin/it)
* Added sync models
  * patio.addModel does not return a promise anymore instead use `patio.syncModel` to sync models with the database
* Added postgres support use `pg://` in connection URI
* Performance Improvements
* Added Text and Blob support
* More test coverage
* Bug Fixes
  * Fixed issue where transaction connection would not always be used on databases
* Added Events to patio

# 0.0.5 / 2012-02-02

* Made "use strict" compatibile

# 0.0.4 / 2012-02-16

* Bug Fixes
   * Fixed issue with closing connections in the Connection Pool
   * Changed association ds rowCb behavior so when using the associated models dataset directly you receive instances back
   * Changed database transaction tracking behavior
   * Added findOrCreate method to query plugin
   * Fixed reload method to clear and reload associations
   * upped comb version to v0.0.9


# 0.0.3 / 2012-02-09

* Bug Fixes
  * Fixed issue in associations where options were not being applied correctly
  * Change Model to return transaction cb error if it was generated otherwise the transaction error.
  * Removed automatic casting to null if property does not exist on current model
  * Table inheritance added check for undefined on the pk and accounted for non autoincrementing ids when creating realationships
  * Changed patio.sql Date wrappers to create a new Date if one is not provided
* Docs fixes

# 0.0.2 / 2012-02-04


  * Added new plugin for model inheritance
    * Added new docs
    * Examples
    * Tests
  * Bug Fixes
     * Fixed issues with logging and external accessibility
     * Fixed concurrent transaction issues where simulaneous transactions would interfere with eachother.
  * Increased performance
  * More documentation
  * More tests
  * New examples
  * Updated example application
  * Model updates
    * Cleaned up model creation
    * Abstracted some core initialization so plugins and subclasses can alter as need be.
  * Added benchmarks for
    * Async inserts
    * Serial inserts
    * Async updates
    * Serial updates
    * Reads
    * Async deletes
    * Serial deletes

# 0.0.1 / 2012-01-31


  * Initial release
