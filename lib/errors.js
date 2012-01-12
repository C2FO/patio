var moose = exports;



/**
 * Thrown if a function is not impltemened
 *
 * @param {String} message the message to show.
 */
moose.NotImplemented = function(message) {
    return new Error("Not Implemented :  " + message);
};

/**
 * Thrown if there is an Expression Error.
 *
 * @param {String} message the message to show.
 */
moose.ExpressionError = function(message) {
    return new Error("Expression Error :" + message);
};

/**
 * Thrown if there is a Query Error.
 *
 * @param {String} message the message to show.
 */
moose.QueryError = function(message){
    return new Error("QueryError : " + message);
};

/**
 * Thrown if there is a Dataset Error.
 *
 * @param {String} message the message to show.
 */
moose.DatasetError = function(message){
    return new Error("DatasetError : " + message);
};

/**
 * Thrown if there is a Database Error.
 *
 * @param {String} message the message to show.
 */
moose.DatabaseError = function(message){
    return new Error("Database error : " + message);
};

/**
 * Thrown if there is a unexpected Error.
 *
 * @param {String} message the message to show.
 */
moose.MooseError = function(message){
    return new Error("Moose error : " + message);
};