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

        return async.waterfall([
            function openDb(cb){
                self._open(cb);
            },
            function clearTables(db,cb){
                return (options.clearAll) ? self._clearTables(null,cb) : cb();
            },
            // function registerSchemas(cb){
            //     return cb();
            // },
            function registerSchemas(cb){
                // queue an operation for creating tables from the
                // default schemas
                var q = async.queue(function(schema,qCb){
                    log.debug('registering schema ' + schema.id );
                    Schema.register(schema);
                    // some schemas don't get instantiated as tables
                    if( schema.persist === false ){ return qCb(); }
                    // log.debug('creating tbl for ' + schema.id );
                    var statement = Schema.toCreate( schema.id );
                    log.debug(statement.get('sql'));
                    self.rawQuery(statement.get('sql'),function(err){
                        if( err ){ log.warn('error creating ' + schema.id +' ' +err);}
                        return qCb(err);
                    });
                },1);

                // log.debug('creating schema tables');
                // print_ins( odgn, 1 );
                // console.log( Schema.schemaTables );
                _.each( Schema.schemaTables, q.push );

                // called when finished
                q.drain = function(){
                    return cb();
                };
            },

        ], callback );
    },

    _clearTables: function(options, callback){
        var self = this;
        log.debug('clearing entity registry' );
        async.eachSeries([
            "PRAGMA writable_schema = 1;",
            "delete from sqlite_master where type = 'table';",
            'PRAGMA writable_schema = 0;',
            "VACUUM"

            // 'DELETE FROM tbl_entity',
            // 'DELETE FROM tbl_component_def'
        ], function(sql,cb){
            self.rawQuery(sql,cb)
        }, function(err){
            if( err ){ log.warn('error clearing ' + err ); }
            return callback();
        });
    },

    /**
     * Checks whether a component table exists
     *
     * 
     * @param  {[type]}   componentDef [description]
     * @param  {Function} callback     [description]
     * @return {[type]}                [description]
     */
    _doesComponentTableExist: function( componentDef, callback ){
        var stmt = Schema.toCheckTableExists( componentDef, {isComponent:true} );
        return this.getRow( stmt.get('sql'), null, function(err,row){
            if( err ){ return callback(err); }
            return callback( null, row ? true : false )
        });
    },

    /**
     * Examines registered schemas and ensures that 
     * tables belong for them
     * 
     * @param  {[type]}   options  [description]
     * @param  {Function} callback [description]
     * @return {[type]}            [description]
     */
    ensureComponentTables: function(options, callback){
        var self = this;
        this._eachComponentDef({}, function(err,componentDef, rowCb){
            // print_ins(arguments);
            // log.debug('hi');
            return self._doesComponentTableExist( componentDef, function(err, exists){
                if( err ){ log.error('error checking com table ' + err ); }
                if( !exists ){
                    // create the table
                    var query = Schema.toCreate( [componentDef, '/schema/component_defaults'], {isComponent:true, debug:true} );
                    log.debug('creating com table ' + query.get('table_name') );
                    self.execQuery( query, rowCb )
                } else {
                    log.debug( componentDef.id + ' exists');
                    return rowCb();
                }
            })

        }, function(err){
            if( err ){ log.error( "something went wrong? " + err ) };
            log.debug('finished ensureComponentTables');
            return callback();
        });
    },



    /**
     * Returns an array of all defined component defs
     * 
     * @param  {Function} callback [description]
     * @return {[type]}            [description]
     */
    _eachComponentDef: function( options, componentDefCallback, callback ){
        return this.eachRow("SELECT * from tbl_component_def WHERE status = 0", null, function(err, row, cb){
            if( err ){ log.debug('error with ' + err ); return callback(err); }
            return componentDefCallback( null, JSON.parse(row.schema), cb );
        }, callback );
    },


    /**
     * [ description]
     * @param  {[type]}   uri      [description]
     * @param  {Function} callback [description]
     * @return {[type]}            [description]
     */
    getComponentDefByUri: function( uri, callback ){
        return this.getRow( "SELECT * FROM tbl_component_def WHERE uri = ? AND status = 0", uri, function(err,row){
            if( err ){ log.debug('gcdbu ' + err ); return callback(err); }
            if( !row ){
                return callback();
            }
            if( row.schema ){
                return callback( null, JSON.parse(row.schema) );
            }
            return callback('invalid comDef ' + row.id );
        });
    },

    /**
     * Registers a Component Def.
     *
     * Creates a new table for the component based on its schema,
     * and then creates a row in tbl_component_def.
     * 
     * @param  {[type]}   componentDef [description]
     * @param  {[type]}   options      [description]
     * @param  {Function} callback     [description]
     * @return {[type]}                [description]
     */
    registerComponent: function( componentDef, options, callback ){
        var self = this;

        // check whether this component is already registered
        // log.debug('retrieving comDef ' + componentDef.schema.id );
        
        return this.getComponentDefByUri( componentDef.schema.id, function(err, def){
            if( def ){
                log.debug('def already registered ' + def.id );
                return callback( null, componentDef );
            }
            // log.debug('getComponentDefByUri returned ' + JSON.stringify(def) );
            // create a table
            
            // process.exit();

            // insert the def in the component_def table
            var query = Schema.toInsert( '/schema/component_def' );

            query.set('uri', componentDef.schema.id );
            query.set('schema', JSON.stringify(componentDef.schema) );

            return self.execQuery( query, function(err, lastId){
                if( err ){ log.error('error inserting component_def: ' + err); print_ins(err.errno); return callback(err); }
                componentDef.id = componentDef.defId = lastId;
                log.debug('inserted new comDef id ' + componentDef.id );

                // we now ensure that all defined components have a table instantiated
                return self.ensureComponentTables( options, function(err){
                    if(err){ log.warn('error ensuring tables ' + err ); }
                    return callback(null,componentDef);    
                });
            });
        });

        // return async.nextTick(function(){
            // print_ins( componentDef.schema );
            // componentDef.id = componentDef.defId = self._componentDefId++;
            // self._componentDefsBySchemaId[ componentDef.schema.id ] = componentDef;
            // self._componentDefs[ componentDef.defId ] = componentDef;
            // return callback(null, componentDef);
        // });
    },



    retrieve: function( query, callback ){
        var sql = _.isString(query) ? query : query.get('sql');
        this.dbHandle.get( sql, callback );
    },



    /**
     * Executes a query against the db that does not return any data
     * 
     * @param  {[type]}   query    [description]
     * @param  {Function} callback [description]
     * @return {[type]}            [description]
     */
    execQuery: function( query, callback ){
        return this.dbHandle.run( query.get('sql'), query.getValues(), function(err){
            if( err ){ return callback(err); }
            return callback( null, this.lastID );
        });
    },

    /**
     * Executes a single one off sql statement
     * 
     * @param  {[type]}   sql      [description]
     * @param  {Function} callback [description]
     * @return {[type]}            [description]
     */
    rawQuery: function( sql, callback ){
        return this.dbHandle.run( sql, function(err){
            if( err ){ return callback(err); }
            return callback();
        });
    },

    getRow: function( sql, params, callback ){
        params = params || [];
        return this.dbHandle.get( sql, params, callback );
    },

    eachRow: function( sql, params, rowCallback, callback ){
        params = params || [];
        var rowCount = 0;
        var totalRowCount = 0;
        // the sqlite driver doesn't wait for row callbacks to
        // finish before calling the complete callback - so we continue on
        var rowCb = function(err,row){
            totalRowCount++;
            return rowCallback(err,row, function(){
                if( err ){ return callback(err); }
                rowCount++;
                if( rowCount == totalRowCount ){
                    return callback();
                }
            });
        };
        var completeCb = function(err){
            // totalRowCount = rowCount;
        };
        return this.dbHandle.each( sql, params, rowCb, completeCb ); //rowCallback, callback );
    },

    _open: function(callback){
        var self = this;
        if( this.dbHandle ){
            return callback(null,this.dbHandle);
        }
        return this.dbHandle = new sqlite3.cached.Database( this.filename, function(err){
            if( err ){ log.error( err ); return callback(err); }
            log.debug('opened db ' + self.filename );
            self.dbHandle.on('profile', function(sql,time){
                log.debug('profile: ' + sql + ' ' + time);
            });
            return callback(null, self.dbHandle);
        });
    },

    _close: function(callback){
        this.dbHandle.close();
        this.dbHandle = null;
        return null;
    }

});