/**
 * @class Thrown if a function is not impltemened
 *
 * @param {String} message the message to show.
 */
export function NotImplemented(message: string): Error;
export class NotImplemented {
    /**
     * @class Thrown if a function is not impltemened
     *
     * @param {String} message the message to show.
     */
    constructor(message: string);
}
/**
 * @class Thrown if there is an Expression Error.
 *
 * @param {String} message the message to show.
 */
export function ExpressionError(message: string): Error;
export class ExpressionError {
    /**
     * @class Thrown if there is an Expression Error.
     *
     * @param {String} message the message to show.
     */
    constructor(message: string);
}
/**
 * @class Thrown if there is a Query Error.
 *
 * @param {String} message the message to show.
 */
export function QueryError(message: string): Error;
export class QueryError {
    /**
     * @class Thrown if there is a Query Error.
     *
     * @param {String} message the message to show.
     */
    constructor(message: string);
}
/**
 * @class Thrown if there is a Dataset Error.
 *
 * @param {String} message the message to show.
 */
export function DatasetError(message: string): Error;
export class DatasetError {
    /**
     * @class Thrown if there is a Dataset Error.
     *
     * @param {String} message the message to show.
     */
    constructor(message: string);
}
/**
 * @class Thrown if there is a Database Error.
 *
 * @param {String} message the message to show.
 */
export function DatabaseError(message: string): Error;
export class DatabaseError {
    /**
     * @class Thrown if there is a Database Error.
     *
     * @param {String} message the message to show.
     */
    constructor(message: string);
}
/**
 * @class Thrown if there is a unexpected Error.
 *
 * @param {String} message the message to show.
 */
export function PatioError(message: string): Error;
export class PatioError {
    /**
     * @class Thrown if there is a unexpected Error.
     *
     * @param {String} message the message to show.
     */
    constructor(message: string);
}
/**
 * @class Thrown if there is a error thrown within a model.
 * @param {String} message the message to show.
 */
export function ModelError(message: string): Error;
export class ModelError {
    /**
     * @class Thrown if there is a error thrown within a model.
     * @param {String} message the message to show.
     */
    constructor(message: string);
}
/**
 * @class Thrown if there is an error when loading/creating/deleteing an association.
 * @param {String} message the message to show.
 */
export function AssociationError(message: string): Error;
export class AssociationError {
    /**
     * @class Thrown if there is an error when loading/creating/deleteing an association.
     * @param {String} message the message to show.
     */
    constructor(message: string);
}
/**
 * Thrown if there is an error when performing a migration.
 * @param {String} message the message to show.
 */
export function MigrationError(message: string): Error;
