#0.1.5 / 2012-09-20
* Fixed issue with sql.literal not accepting a single identifier
* Fixed Issue with one to many model if the table is plurarlized
* Fixed issue with Dataset#group not using stringToIdentifier to convert column arguments to identifiers
* Cleaned up model code to use async array and chain methods so errors are propagated properly.
* updated timestamp plugin to set updated/created when just retreving sql
* Fixed primary key caching issue

#0.1.4 / 2012-09-20
* added a rowCB for custom dataset model
* added insert, update and remove sql properties on models
* re-added jscoverage submodule


#0.1.3 / 2012-09-10
* Added travis CI
* Code clean up
* Updated comb version

#0.1.2 / 2012-09-06
* Updated for new comb api.
* Fixed tests
* Update db.tranaction to require a promise or the callback function to be called.

#0.1.1 / 2012-08-29
* new patio.Dataset features
  * sourceList - get all sources as identifiers
  * joinSourceList - get all join sources
  * hasSelectSource - returns true if there are not any select sources (i.e select *)
  * seleIfNoSource - add the selects if there is not currently a select
* Fixed issue with patio.Model#_setFromDb where values not in the models table columns would be in accesible, (i.e a join with a model would not show the join columns)
* updated docs

#0.1.0 / 2012-08-25
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
   

#0.0.9 / 2012-08-16
* Fixed bug where default that are buffers were not handle properly (#35)
* Fixed bug with inheritance loading
* Updated benchmark
* Changed `oneToMany` to load models when removing to call hooks
* New ColumnMapper plugin
* added alwaysQualify which will qualify a dataset every time sql is generated from it.
* converted model to use comb.serial instead of the promise chaining api
* changed reload of save and update to be invoked after post save and update have been called.
* Merged pull request from @mbenedettini to lazy initialize hive

#0.0.8 / 2012-07-13

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

#0.0.5 / 2012-02-02

* Made "use strict" compatibile

#0.0.4 / 2012-02-16

* Bug Fixes
   * Fixed issue with closing connections in the Connection Pool
   * Changed association ds rowCb behavior so when using the associated models dataset directly you receive instances back
   * Changed database transaction tracking behavior
   * Added findOrCreate method to query plugin
   * Fixed reload method to clear and reload associations
   * upped comb version to v0.0.9
   

#0.0.3 / 2012-02-09

* Bug Fixes
  * Fixed issue in associations where options were not being applied correctly
  * Change Model to return transaction cb error if it was generated otherwise the transaction error.
  * Removed automatic casting to null if property does not exist on current model
  * Table inheritance added check for undefined on the pk and accounted for non autoincrementing ids when creating realationships
  * Changed patio.sql Date wrappers to create a new Date if one is not provided
* Docs fixes

#0.0.2 / 2012-02-04


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

#0.0.1 / 2012-01-31


  * Initial release