var sqlite3 = require('sqlite3').verbose();
var Schema = require('./schema');
var odgnEntitySchema = odgn.entity.Schema;
Schema.setup( odgnEntitySchema );

module.exports = {
    create:function(registry, options){
        options = options || {};
        var result = new SqliteStorage(options);
        result.registry = registry;
        return result;
    }
};


var SqliteStorage = function(options){
    this.filename = options.filename || ':memory:';
};


_.extend(SqliteStorage.prototype, {

    initialise: function(options,callback){
        var self = this;//, db = this.db;
        log.debug('initialising sqlite entity registry');

        var createSchemaTables = function(){
            self._open(function(err,db){
                if( err ){ return callback(err); }
                // queue an operation for creating tables from the
                // default schemas
                var q = async.queue(function(schema,cb){
                    log.debug('registering schema ' + schema.id );
                    Schema.register(schema);
                    var sql = Schema.toCreate( schema.id );
                    log.debug(sql);
                    db.run(sql,function(err){
                        if( err ){ log.warn('error creating ' + schema.id +' ' +err);}
                        cb(err);
                    });
                },1);

                // log.debug('creating schema tables');
                // print_ins( odgn, 1 );
                // console.log( Schema.schemaTables );
                _.each( Schema.schemaTables, q.push );

                // called when finished
                q.drain = function(){
                    callback();
                };
            });
        };

        if( options.clearAll ){
            log.debug('clearing entity registry' );
            self._open( function(err,db){
                db.exec('DELETE FROM tbl_entity;', function(err){
                    if( err ){ log.error(err); } //return callback(err); }
                    return createSchemaTables();
                });
            });
            
        } else
            return createSchemaTables();
    },

    /**
     * [ description]
     * @param  {[type]}   componentDef [description]
     * @param  {[type]}   options      [description]
     * @param  {Function} callback     [description]
     * @return {[type]}                [description]
     */
    registerComponent: function( componentDef, options, callback ){
        var self = this;
        self._open(function(err,db){
            if( err ){ return callback(err); }
            var query = Schema.toInsert( '/schema/component_def' );
            
            log.debug('insert component ' + query.get('sql') );

            query.set('uri', componentDef.schema.id );
            query.set('schema', JSON.stringify(componentDef.schema) );

            // print_ins( query ); //odgnEntitySchema.getProperties(componentDef.schema) );

            db.run(query.get('sql'), query.getValues(), function(err){
                if( err ){
                    return callback(err);
                }
                process.exit();
            //     var entity = registry._create();
            //     entity.id = this.lastID;
            //     return callback(null,entity);
            });
        });

        return async.nextTick(function(){
            print_ins( componentDef.schema );
            // componentDef.id = componentDef.defId = self._componentDefId++;
            // self._componentDefsBySchemaId[ componentDef.schema.id ] = componentDef;
            // self._componentDefs[ componentDef.defId ] = componentDef;
            // return callback(null, componentDef);
        });
    },

    _open: function(callback){
        var self = this;
        return this.dbHandle = new sqlite3.cached.Database( this.filename, function(err){
            if( err ){ return callback(err); }
            return callback(null, self.dbHandle);
        });
    },

    _close: function(callback){
        this.dbHandle.close();
        this.dbHandle = null;
        return null;
    }

});




var oldExports /*module.exports*/ = function(odgn, options){
    var options = options || {};
    var Entity = odgn.entity || odgn;
    odgn.entity.Sqlite = {};
    var Schema = odgn.entity.Sqlite.Schema = require('./schema')(Entity);

    var dbFilename = options.filename || ':memory:';

    var dbHandle;
    var dbOpen = function(callback){
        return dbHandle = new sqlite3.cached.Database( dbFilename, function(err){
            if( err ){
                return callback(err);
            }
            return callback(null,dbHandle);
        });
    };

    var dbClose = function(){
        return dbHandle.close();
    }
    
    // log.debug('using ' + Entity.EntityRegistry );
    // print_ins( Entity.EntityRegistry );


    var EntityStorage = function(){
    };

    EntityStorage.prototype.initialise = function(options,callback){
        var self = this;//, db = this.db;
        log.debug('initialising sqlite entity registry');

        var createSchemaTables = function(){
            dbOpen(function(err,db){
                if( err ){
                    return callback(err);
                }

                // queue an operation for creating tables from the
                // default schemas
                var q = async.queue(function(schema,cb){
                    Schema.register(schema);
                    var sql = Schema.toCreate( schema.id );
                    log.debug(sql);
                    db.run(sql,function(err){
                        if( err ){ log.warn('error creating ' + schema.id +' ' +err);}
                        cb(err);
                    });
                },1);

                _.each( Schema.schemas, q.push );

                // called when finished
                q.drain = function(){
                    callback(null,registry);
                };
            });
        };

        if( options.clearAll ){
            log.debug('clearing entity registry' );
            dbOpen( function(err,db){
                db.exec('DELETE FROM tbl_entity; DELETE FROM tbl_component_def;', function(err){
                    return createSchemaTables();
                });
            });
            
        } else
            return createSchemaTables();
    };

    /**
     * Creating a new entity
     * @param  {[type]}   options  [description]
     * @param  {Function} callback [description]
     * @return {[type]}            [description]
     */
    EntityStorage.prototype.create = function( registry, options, callback){
        var self = this;
        if( _.isFunction(options) ){
            callback = options; options = {};
        }
        
        if( options.clearingAll ){
            log.debug('clearing all');
        }
        dbOpen(function(err,db){
            if( err ){
                return callback(err);
            }
            db.run('INSERT INTO tbl_entity (status) VALUES (?);', [0], function(err){
                if( err ){
                    return callback(err);
                }
                var entity = registry._create();
                entity.id = this.lastID;
                return callback(null,entity);
            });
        });
    };

    EntityStorage.prototype.read = function( entityId, registry, options, callback ){
        var self = this;

        dbOpen(function(err,db){
            if( err ){
                return callback(err);
            }
            return db.get('SELECT * FROM tbl_entity WHERE id = ? LIMIT 1', [entityId], function(err,row){
                if( err ){
                    return callback(err);
                }
                var entity = registry._parse( row );
                callback( null, entity );
            });
        });
    };

    var removeAllTables = function(options, callback){
        if( arguments.length == 1 ){
            callback = options;
            options = {};
        }
        var self = this;
        log.debug('removing all tables');
        dbOpen(function(err,db){
            if( err ){
                return callback(err);
            }
            async.waterfall([
                function(cb){
                    db.all("SELECT name FROM sqlite_master WHERE type = 'table'", cb);
                },
                function(rows,cb){
                    async.eachSeries( rows, function(row,rowCb){
                        log.debug('removing table ' + row.name);
                        db.get("DROP TABLE " + row.name,rowCb);
                    }, cb );
                }
            ], callback );
        });
    };

    var result = {
        Schema: require('./schema')(Entity),
        EntityRegistry: EntityStorage,
        ComponentRegistry: require('./component')(odgn,{dbOpen:dbOpen, dbClose:dbClose}).ComponentStorage,
        util:{
            clearAll: removeAllTables
        }
    };

    return result;
};