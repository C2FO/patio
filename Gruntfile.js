/*global module:false*/
module.exports = function (grunt) {
    // Project configuration.

    // Automatic module definition loading. Significantly speeds up build cycles
    require('jit-grunt')(grunt);

    // Time how long tasks take. Can help when optimizing build times
    require('time-grunt')(grunt);

    // Project configuration.
    var DEFAULT_COVERAGE_ARGS = ["cover", "-x", "Gruntfile.js", "--report", "none", "--print", "none", "--include-pid", "grunt", "--", "it"];

    grunt.initConfig({
        pkg: grunt.file.readJSON('package.json'),

        patio: {
            paths: {
                root: './',
                lib: './lib',
                test: './test'
            }
        },

        jshint: {
            src: [
                "./index.js",
                "<%= patio.paths.lib %>/**/*.js",
                "<%= patio.paths.test %>/**/*.js",
                "Gruntfile.js"
            ],
            options: {
                jshintrc: '.jshintrc'
            }
        },

        exec: {
            sendToCoveralls: "cat ./coverage/lcov.info | ./node_modules/coveralls/bin/coveralls.js",
            removeCoverage: "rm -rf ./coverage",
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

    grunt.registerTask("spawn-test", "spawn tests", function (db) {
        var done = this.async();
        var env = process.env;
        env.PATIO_DB = db;
        grunt.util.spawn({cmd: "grunt", args: ["it"], opts: {stdio: 'inherit', env: env}}, function (err) {
            if (err) {
                done(false);
            } else {
                done();
            }
        });
    });

    grunt.registerTask("spawn-test-coverage", "spawn tests with coverage", function (db) {
        var done = this.async();
        var env = process.env;
        env.PATIO_DB = db;
        grunt.util.spawn({cmd: "istanbul", args: DEFAULT_COVERAGE_ARGS, opts: {stdio: 'inherit', env: env}}, function (err) {
            if (err) {
                done(false);
            } else {
                done();
            }
        });
    });

    grunt.registerTask("process-coverage", "process coverage obects", function () {
        var files = grunt.file.expand("./coverage/coverage*.json"),
            istanbul = require('istanbul'),
            collector = new istanbul.Collector(),
            reporter = new istanbul.Reporter(),
            sync = false,
            done = this.async();

        files.forEach(function (file) {
            collector.add(grunt.file.readJSON(file));
        });

        reporter.add('text');
        reporter.addAll(['lcovonly']);
        reporter.write(collector, sync, function (err) {
            if (err) {
                console.error(err.stack);
                return done(false);
            }
            console.log('All reports generated');
            done();
        });
    });

    // Default task.
    grunt.registerTask('default', ['jshint', "test", "test-coverage", "docs"]);

    grunt.registerTask('test', ['jshint', 'test-mysql', 'test-pg']);
    grunt.registerTask('test-mysql', ['jshint', "spawn-test:mysql"]);
    grunt.registerTask('test-pg', ['jshint', "spawn-test:pg"]);

    grunt.registerTask('test-coverage', ['exec:removeCoverage', 'test-mysql-coverage', 'test-pg-coverage', 'process-coverage', 'exec:sendToCoveralls', 'exec:removeCoverage']);
    grunt.registerTask('test-mysql-coverage', ["spawn-test-coverage:mysql"]);
    grunt.registerTask('test-pg-coverage', ["spawn-test-coverage:pg"]);

    grunt.registerTask("docs", ["exec:removeDocs", "exec:createDocs"]);

    grunt.loadNpmTasks('grunt-it');
    grunt.loadNpmTasks('grunt-contrib-jshint');
    grunt.loadNpmTasks('grunt-exec');
    grunt.loadNpmTasks('grunt-coveralls');


};
