var comb = require("comb"),
    toArray = comb.array.toArray,
    isInstanceOf = comb.isInstanceOf,
    isBoolean = comb.isBoolean,
    when = comb.when,
    isHash = comb.isHash,
    isString = comb.isString,
    sql = require("../sql.js").sql,
    AliasedExpression = sql.AliasedExpression,
    Identifier = sql.Identifier,
    isConditionSpecifier = sql.Expression.isConditionSpecifier,
    ModelError = require("../errors.js").ModelError;


/**
 * @class This plugin exposes the ability to map columns on other tables to this Model.
 *
 * See {@link patio.plugins.ColumnMapper.mappedColumn} for more information.
 *
 * @name ColumnMapper
 * @memberof patio.plugins
 *
 */
comb.define(null, {

    "static": {

        /**@lends patio.plugins.ColumnMapper*/

        /**
         * Boolean flag indicating if mapped columns should be re-fetched on update.
         *
         * <b>NOTE</b> This can be overridden by passing {reload : false} to the {@link patio.Model#update} method.
         * @default true
         */
        fetchMappedColumnsOnUpdate: true,

        /**
         * Boolean flag indicating if mapped columns should be re-fetched on save.
         *
         * <b>NOTE</b> This can be overridden by passing {reload : false} to the {@link patio.Model#save} method.
         * @default true
         */
        fetchMappedColumnsOnSave: true,


        /**
         * Add a mapped column from another table. This is useful if there columns on
         * another table but you do not want to load the association every time.
         *
         *
         * For example assume we have an employee and works table. Well we might want the salary from the works table,
         * but do not want to add it to the employee table.
         *
         * <b>NOTE:</b> mapped columns are READ ONLY.
         *
         * {@code
         * patio.addModel("employee")
         *    .oneToOne("works")
         *    .mappedColumn("salary", "works", {employeeId : patio.sql.identifier("id")});
         * }
         *
         * You can also change the name of the of the column
         *
         * {@code
         *  patio.addModel("employee")
         *    .oneToOne("works")
         *    .mappedColumn("mySalary", "works", {employeeId : patio.sql.identifier("id")}, {
         *          column : "salary"
         *    });
         * }
         *
         * If you want to prevent the mapped columns from being reloaded after a save or update you can set the
         * <code>fetchMappedColumnsOnUpdate</code> or <code>fetchMappedColumnsOnSave</code> to false.
         *
         * {@code
         *
         * var Employee = patio.addModel("employee")
         *   .oneToOne("works")
         *   .mappedColumn("mySalary", "works", {employeeId : patio.sql.identifier("id")}, {
         *          column : "salary"
         *   });
         *
         * //prevent the mapped columns from being fetched after a save.
         * Employee.fetchMappedColumnsOnSave = false;
         *
         * //prevent the mapped columns from being re-fetched after an update.
         * Employee.fetchMappedColumnsOnUpdate = false;
         * }
         *
         * You can also override prevent the properties from being reloaded by setting the <code>reload</code> or <code>reloadMapped</code> options when saving or updating.
         *
         * {@code
         * //prevents entire model from being reloaded including mapped columns
         * employee.save(null, {reload : false});
         * employee.update(null, {reload : false});
         *
         * //just prevents just the mapped columns from being reloaded
         * employee.save(null, {reloadMapped : false});
         * employee.update(null, {reloadMapped : false});
         * }
         *
         * @param {String} name the name you want the column represented as on the model.
         * @param {String|patio.Model} table the table or model you want the property mapped from
         * @param condition the join condition. See {@link patio.Dataset#joinTable}.
         * @param {Object} [opts={}] additional options
         * @param {String} [opts.joinType="left"] the join type to use when gathering the properties.
         * @param {String|patio.sql.Identifer} [opts.column=null] the column on the remote table that should be used
         * as the local copy.
         *
         * @return {patio.Model} returns the model for chaining.
         */
        mappedColumn: function (name, table, condition, opts) {
            opts = opts || {};
            if (name) {
                name = sql.stringToIdentifier(name);
                if (table && condition) {
                    opts = comb.merge({joinType: "left", table: table, condition: condition, column: name}, opts);
                    this._mappedColumns[name] = opts;
                } else {
                    throw new ModelError("mapped column requires a table and join condition");
                }
            }
            return this;
        },

        sync: function () {
            var ret = this._super(arguments);
            if (!this.synced) {
                var self = this;
                ret = ret.chain(function () {
                    var ds = self.dataset.naked().select(), mappedColumns = self._mappedColumns, joinTableAlias = "columnMapper", selects = [];
                    Object.keys(mappedColumns).forEach(function (column, i) {
                        var opts = mappedColumns[column],
                            condition = opts.condition;
                        if (isConditionSpecifier(condition)) {
                            condition = toArray(condition);
                            condition.forEach(function (cond) {
                                var val = cond[1];
                                if (!isInstanceOf(val, AliasedExpression) && isInstanceOf(val, Identifier)) {
                                    cond[1] = val.qualify(self.tableName);
                                }
                            });
                            var tableAlias = sql.identifier(joinTableAlias + i);
                            ds = ds.joinTable(opts.joinType, opts.table, condition, {tableAlias: tableAlias});
                            selects.push(sql.stringToIdentifier(opts.column).qualify(tableAlias).as(column));
                        }
                    });
                    ds = ds.select(selects);

                    var middleWare = function (next) {
                        var self = this;
                        ds.filter(this._getPrimaryKeyQuery()).qualify().one().chain(function (vals) {
                            self.setValues(vals);
                            return vals;
                        }).classic(next);
                    };

                    self.post("load", middleWare);
                    self.post("save", function (next, options) {
                        options = options || {};
                        if (self.fetchMappedColumnsOnSave &&
                            isBoolean(options.reload) ? options.reload :
                            isBoolean(options.reloadMapped) ? options.reloadMapped :
                                self.reloadOnSave) {
                            middleWare.apply(this, arguments);
                        } else {
                            next();
                        }
                    });
                    self.post("update", function (next, options) {
                        options = options || {};
                        if (self.fetchMappedColumnsOnUpdate &&
                            isBoolean(options.reload) ? options.reload :
                            isBoolean(options.reloadMapped) ? options.reloadMapped :
                                self.reloadOnUpdate) {
                            middleWare.apply(this, arguments);
                        } else {
                            next();
                        }
                    });
                });
            }
            return ret;
        },

        init: function () {
            this._super(arguments);
            this._mappedColumns = {};
        }

    }

}).as(module);