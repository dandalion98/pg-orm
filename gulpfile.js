let gulp = require('gulp'),
    path = require('path'),
    mocha = require('gulp-mocha');

gulp.task('test', function (done) {
    console.log("starting test: " + process.pid)

    var testSuites = ['tests/test.*.js'];
    var error;
    console.dir(testSuites)

    gulp.src(testSuites)
        .pipe(mocha({
            reporter: 'spec',
            timeout: 10000,
            exit: true
        }))
        .on('error', function (err) {
            console.log("err")
            error = err;
            log.error(error.stack)
        })
        .on('end', function () {
            console.log("end")
            return done(error);
        });
});