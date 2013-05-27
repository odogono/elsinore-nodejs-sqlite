async = require('async');
odgn = require('../../odgn-entity')();
assert = require('assert');
path = require('path');
util = require('util');

sh = require('shelljs');

root.print_ins = function(arg,depth,showHidden,colors){
    if( _.isUndefined(depth) ) depth = 5;
    util.log( util.inspect(arg,showHidden,depth,colors) );
};

root.print_var = function(arg, options){
    util.log( JSON.stringify(arg,null,'\t') );
}

root.log = {
    debug: util.log
}