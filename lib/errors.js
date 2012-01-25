var patio = exports;


/**
 * Thrown if a function is not impltemened
 *
 * @param {String} message the message to show.
 */
patio.NotImplemented = function(message) {
  return new Error("Not Implemented :  " + message);
};

/**
 * Thrown if there is an Expression Error.
 *
 * @param {String} message the message to show.
 */
patio.ExpressionError = function(message) {
  return new Error("Expression Error :" + message);
};

/**
 * Thrown if there is a Query Error.
 *
 * @param {String} message the message to show.
 */
patio.QueryError = function(message) {
  return new Error("QueryError : " + message);
};

/**
 * Thrown if there is a Dataset Error.
 *
 * @param {String} message the message to show.
 */
patio.DatasetError = function(message) {
  return new Error("DatasetError : " + message);
};

/**
 * Thrown if there is a Database Error.
 *
 * @param {String} message the message to show.
 */
patio.DatabaseError = function(message) {
  return new Error("Database error : " + message);
};

/**
 * Thrown if there is a unexpected Error.
 *
 * @param {String} message the message to show.
 */
patio.PatioError = function(message) {
  return new Error("Patio error : " + message);
};

/**
 * Thrown if there is a error thrown within a model.
 * @param {String} message the message to show.
 */
exports.ModelError = function(message) {
  return new Error("Model error : " + message);
};

/**
 * Thrown if there is an error when loading/creating/deleteing an association.
 * @param {String} message the message to show.
 */
exports.AssociationError = function(message) {
  return new Error("Association error : " + message);
};

