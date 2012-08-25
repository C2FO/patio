
#Logging

Patio uses the [comb.logging](http://pollenware.github.com/comb/symbols/comb.logging.Logger.html) framework for all logging. To set up logging there are two scenarios.

If you installed comb at the root of your project and then installed patio

`npm install comb patio`


Then patio should be using the same version of comb that your application uses. If that is the case then you can configure logging through

* [comb.logging.BasicConfigurator](http://pollenware.github.com/comb/symbols/comb.logging.BasicConfigurator.html)
* [comb.logging.PropertyConfigurator](http://pollenware.github.com/comb/symbols/comb.logging.PropertyConfigurator.html)

```
var comb = require("comb"),
    patio = require("patio");                            

comb.logger.configure();
comb.logger("patio").level = "info";
```

Or
```
//configure with a JSON file.
comb.logger.configure("/location/of/log/config.json");

//or

var loggingConfig = {
    //set the root patio logger to INFO by setting it on the root patio logger, patio.Dataset, patio.Database will
    //all get set to INFO level.
    "patio" : {
        level : "INFO",
        appenders : [
            {
                type : "ConsoleAppender"
            }
        ]
    },
    //set the database logger to DEBUG
    "patio.Database" : {
        level : "DEBUG",
        appenders : [
            {
                type : "ConsoleAppender"
            }
        ]
    }
};
comb.logger.configure(loggingConfig);
```

If you are using a different version of comb or do not want to use comb, then you can use the following method [patio.configureLogging](./patio.html#configureLogging);

```
var comb = require("comb"),
    patio = require("patio");
//sets up a basic configurator
patio.configureLogging();
```

Or with a JSON file
```
patio.configureLogging("/location/of/log/config.json");
```

Or with an object

```
var loggingConfig = {
    //set the root patio logger to INFO by setting it on the root patio logger, patio.Dataset, patio.Database will
    //all get set to INFO level.
    "patio" : {
        level : "INFO",
        appenders : [
            {
                type : "ConsoleAppender"
            },
            {
                type:"RollingFileAppender",
                file:"/var/log/patio.log"
            }
        ]
    },
    //set the database logger to DEBUG
    "patio.Database" : {
        level : "DEBUG",
        appenders : [
            {
                type : "ConsoleAppender"
            }
        ]
    }
};
patio.configureLogging(loggingConfig);
```

The patio logger currently contains the following loggers:

* patio
* patio.Dataset
* patio.Database

To get access to patios root logger use the `patio.LOGGER` property.

```
var patioLogger = patio.LOGGER;
patioLogger.level = "off";
```

There are also a methods for each logger method on patio.

```
patio.logDebug("DEBUG");
patio.logInfo("INFO");
patio.logTrace("TRACE");
patio.logError("ERROR");
patio.logWarn("WARN");
patio.logFatal("FATAL");
```
