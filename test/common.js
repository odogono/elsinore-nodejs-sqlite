
async = require('async');
odgnEntity = require('../../odgn-entity');
assert = require('assert');
path = require('path');
util = require('util');
sinon = require('sinon');
sh = require('shelljs');
fs = require('fs');

var rootDir = path.join( path.dirname(__filename), '../' );

Common = {
    paths:{ 
        root: rootDir,
        fixtures: path.join( rootDir, 'test', 'fixtures' )
    }
};

Common.path = function( dir, subPath ){
    return path.join( Common.paths[dir], subPath );
};

Common.pathVar = function( p ){
    return path.join( sh.tempdir(), p );
}

Common.pathFixture = function( subPath ){
    return path.join( Common.paths.fixtures, subPath );
};

Common.readFixture = function( subPath, parseJson ){
    var fixturePath = Common.pathFixture( subPath );
    var data = fs.readFileSync( fixturePath, 'utf8' );
    return parseJson ? JSON.parse(data) : data;
}


root.print_ins = function(arg,depth,showHidden,colors){
    if( _.isUndefined(depth) ) depth = 1;
    var stack = __stack[1];
    var fnName = stack.getFunctionName();
    var line = stack.getLineNumber();
    // util.log( fnName + ':' + line + ' ' + util.inspect(arg,showHidden,depth,colors) );
    util.log( util.inspect(arg,showHidden,depth,colors) );
};

root.print_var = function(arg, options){
    var stack = __stack[1];
    var fnName = stack.getFunctionName();
    var line = stack.getLineNumber();
    // util.log( fnName + ':' + line + ' ' + JSON.stringify(arg,null,'\t') );
    util.log( JSON.stringify(arg,null,"\t") );
}

root.log = {
    warn:util.log,
    error: util.log,
    debug: util.log,
    info: util.log
}

root.print_stack = function(){
    var rootPath = path.join(path.dirname(__filename),'../');
    var stack = _.map(__stack, function(entry,i){
        var filename = entry.getFileName();
        if( filename.indexOf(rootPath) === 0  ){
            filename = filename.substring(rootPath.length);
        }
        return _.repeat("  ", i) + filename + ' ' + entry.getFunctionName() + ':' + entry.getLineNumber()
    });
    stack.shift();
    util.log( "\n" + stack.join("\n") );
}

Object.defineProperty(global, '__stack', {
get: function() {
        var orig = Error.prepareStackTrace;
        Error.prepareStackTrace = function(_, stack) {
            return stack;
        };
        var err = new Error;
        Error.captureStackTrace(err, arguments.callee);
        var stack = err.stack;
        Error.prepareStackTrace = orig;
        return stack;
    }
});

Object.defineProperty(global, '__line', {
get: function() {
        return __stack[1].getLineNumber();
    }
});

Object.defineProperty(global, '__function', {
get: function() {
        return __stack[1].getFunctionName();
    }
});

global.Storage = require('../');
global.Schema = require('../lib/schema');
global.initRegistryWithSql = function( sqlitePath, sqlFixturePath, callback ){
    var registry, storage;
    Storage.loadSql( sqlitePath, fs.readFileSync( Common.pathFixture(sqlFixturePath)).toString(), function(err, storage){
        odgnEntity.Registry.create({initialize:true, storage:storage, verbose:true, filename:sqlitePath}, function(err, registry){
            if( err ) throw err;
            return callback( err, registry, registry.storage );
        });
    });
};


global.initRegistryWithComponentsAndImport = function( sqlitePath, componentPath, importDataPath, callback ){
    odgnEntity.Registry.create({initialize:true, storage:Storage, verbose:true, filename:sqlitePath, clearAll:true}, function(err, registry){
        var components = Common.readFixture( componentPath, true );
        return registry.registerComponent( components, null, function(){

            var data = Common.readFixture( importDataPath, true);
            return registry.importEntity( data, null, function(err, entity){
                return callback( err, registry, registry.storage );
            });
        });
    });
}