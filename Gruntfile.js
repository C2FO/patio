/*global module:false*/
module.exports = function (grunt) {
    // Project configuration.
    grunt.initConfig({
        pkg: grunt.file.readJSON('package.json'),
        jshint: {
            src: ["./index.js", "lib/**/*.js", "Gruntfile.js"],
            options: {
                jshintrc: '.jshintrc'
            }
        },

        exec: {
            runMySqlCov: "export NODE_PATH=lib-cov:$NODE_PATH && export NODE_ENV=test-coverage &&  export PATIO_DB=mysql && ./node_modules/it/bin/it -r dotmatrix --cov-html ./docs-md/coverage.html",
            runPsqlCov: "export NODE_PATH=lib-cov:$NODE_PATH && export NODE_ENV=test-coverage &&  export PATIO_DB=pg && ./node_modules/it/bin/it -r dotmatrix --cov-html ./docs-md/coverage.html",
            installCoverage: "cd support/jscoverage && ./configure && make && mv jscoverage node-jscoverage",
            createCoverage: "rm -rf ./lib-cov && node-jscoverage ./lib ./lib-cov",
            removeDocs: "rm -rf docs/*",
            createDocs: 'coddoc -f multi-html -d ./lib --dir ./docs'
        },

        it: {
            all: {
                src: 'test/**/*.test.js',
                options: {
                    timeout: 3000, // not fully supported yet
                    reporter: 'tap'
                }
            }
        }
    });

    grunt.registerTask('test_mysql', 'set PATIO_DB to mysql', function () {
        var done = this.async();
        var env = process.env;
        env.NODE_PATH = "./lib";
        env.PATIO_DB = "mysql";
        grunt.util.spawn({cmd: "grunt", args: ["it"], opts: {stdio: 'inherit', env: env}}, function (err) {
            if (err) {
                done(false);
            } else {
                done();
            }
        });
    });

    grunt.registerTask('test_psql', 'set PATIO_DB to pg', function () {
        var done = this.async();
        var env = process.env;
        env.NODE_PATH = "./lib";
        env.PATIO_DB = "pg";
        grunt.util.spawn({cmd: "grunt", args: ["it"], opts: {stdio: 'inherit', env: env}}, function (err) {
            if (err) {
                done(false);
            } else {
                done();
            }
        });
    });

    grunt.registerTask('test_psql_cov', 'set PATIO_DB to pg', function () {
        var done = this.async();
        var env = process.env;
        env.NODE_PATH = "./lib-cov";
        env.NODE_ENV = "test-coverage";
        env.PATIO_DB = "pg";
        grunt.util.spawn({cmd: "grunt", args: ["it"], opts: {stdio: 'inherit', env: env}}, function (err) {
            if (err) {
                done(false);
            } else {
                done();
            }
        });
    });

    grunt.registerTask('test_mysql_cov', 'set PATIO_DB to pg', function () {
        var done = this.async();
        var env = process.env;
        env.NODE_PATH = "./lib-cov";
        env.NODE_ENV = "test-coverage";
        env.PATIO_DB = "mysql";
        grunt.util.spawn({cmd: "grunt", args: ["it"], opts: {stdio: 'inherit', env: env}}, function (err) {
            if (err) {
                done(false);
            } else {
                done();
            }
        });
    });

    // Default task.
    grunt.registerTask('default', ['jshint', "test", "test-coverage", "docs"]);
    grunt.registerTask("test-coverage", ["exec:createCoverage", "exec:runMySqlCov", "exec:runPsqlCov"]);
    grunt.registerTask("docs", ["exec:removeDocs", "exec:createDocs"]);
    grunt.loadNpmTasks('grunt-it');
    grunt.loadNpmTasks('grunt-contrib-jshint');
    grunt.registerTask('mysql', ['jshint', 'mysql_env', 'it']);
    grunt.registerTask('pg', ['jshint', 'test_psql']);
    grunt.registerTask('test', ['jshint', 'test_mysql', 'test_psql']);
    grunt.loadNpmTasks('grunt-exec');
};
