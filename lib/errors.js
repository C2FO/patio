/**
 * @class Thrown if a function is not impltemened
 *
 * @param {String} message the message to show.
 */
export function NotImplemented (message) {
    return new Error("Not Implemented :  " + message);
}

/**
 * @class Thrown if there is an Expression Error.
 *
 * @param {String} message the message to show.
 */
export function ExpressionError (message) {
    return new Error("Expression Error :" + message);
};

/**
 * @class Thrown if there is a Query Error.
 *
 * @param {String} message the message to show.
 */
export function QueryError (message) {
    return new Error("QueryError : " + message);
};

/**
 * @class Thrown if there is a Dataset Error.
 *
 * @param {String} message the message to show.
 */
export function DatasetError (message) {
    return new Error("DatasetError : " + message);
};

/**
 * @class Thrown if there is a Database Error.
 *
 * @param {String} message the message to show.
 */
export function DatabaseError (message) {
    return new Error("Database error : " + message);
};

/**
 * @class Thrown if there is a unexpected Error.
 *
 * @param {String} message the message to show.
 */
export function PatioError (message) {
    return new Error("Patio error : " + message);
};

/**
 * @class Thrown if there is a error thrown within a model.
 * @param {String} message the message to show.
 */
export function ModelError (message) {
    return new Error("Model error : " + message);
};

/**
 * @class Thrown if there is an error when loading/creating/deleteing an association.
 * @param {String} message the message to show.
 */
export function AssociationError (message) {
    return new Error("Association error : " + message);
};


/**
 * Thrown if there is an error when performing a migration.
 * @param {String} message the message to show.
 */
export function MigrationError (message) {
    return new Error("Migration error : " + message);
};
