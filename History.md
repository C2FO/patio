0.0.4 / 2012-02-16
===
* Bug Fixes
   * Fixed issue with closing connections in the Connection Pool
   * Changed association ds rowCb behavior so when using the associated models dataset directly you receive instances back
   * Changed database transaction tracking behavior
   * Added findOrCreate method to query plugin
   * Fixed reload method to clear and reload associations
   * upped comb version to v0.0.9
   

0.0.3 / 2012-02-09
===
* Bug Fixes
  * Fixed issue in associations where options were not being applied correctly
  * Change Model to return transaction cb error if it was generated otherwise the transaction error.
  * Removed automatic casting to null if property does not exist on current model
  * Table inheritance added check for undefined on the pk and accounted for non autoincrementing ids when creating realationships
  * Changed patio.sql Date wrappers to create a new Date if one is not provided
* Docs fixes

0.0.2 / 2012-02-04
==================

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

0.0.1 / 2012-01-31
==================

  * Initial release