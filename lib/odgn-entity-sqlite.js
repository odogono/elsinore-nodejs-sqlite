var sqlite3 = require('sqlite3').verbose();
var Schema = require('./schema');
var odgnEntitySchema = odgnEntity.Schema;
Schema.setup( odgnEntity.Schema );

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


_.extend(SqliteStorage.prototype, Backbone.Events, {

    initialize: function(options,callback){
        var self = this;//, db = this.db;
        log.debug('initialising sqlite entity registry');

        // records the component defs (string) schema id against the (int) component defs db id
        self.componentDefSchemaIds = []; //[ componentDef.id ] = componentDef.schema.id;
        self.componentDefIds = [];

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
                    // log.debug(statement.get('sql'));
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

    /**
     * 
     * @param  {[type]}   entity   [description]
     * @param  {[type]}   options  [description]
     * @param  {Function} callback [description]
     * @return {[type]}            [description]
     */
    createEntity: function(entity, options, callback){
        var self = this;
        var Entity = odgnEntity.Entity;
        entity = Entity.toEntity(entity);

        var query = Schema.toInsert( '/schema/entity', {} );
        query.set( entity.toJSON() );
        // log.debug('createEntity ' + query.get('sql') );
        // print_ins( query.getValues() );
        // print_ins( entity.toJSON() ) ;
        // print_ins( entity );
        // process.exit();


        return self.execQuery( query, {}, function(err, lastId){
            if( err ){ log.error('error inserting entity: ' + err); print_ins(err.errno); return callback(err); }
            entity.id = lastId;
            log.debug('inserted new entity id ' + entity.id );

            return callback(null, entity);
        });
    },

    /**
     * If an entity exists, returns the entity id
     * @param  {[type]} entityId [description]
     * @return {[type]}          [description]
     */
    hasEntity: function( entityId, callback ){
        var self = this;

        return this.getRow( "SELECT * FROM tbl_entity WHERE id = ?", entityId, function(err,row){
            if( err ){ log.debug('hasEntity ' + err ); return callback(err); }
            if( !row ){
                return callback();
            }
            return callback( null, row.id );
        });
    },

    destroyEntity: function(entity, options, callback ){
        var self = this;
        
        entity = Entity.toEntity(entity);

        throw new Error('not yet implemented');
        // return async.nextTick(function(){
        //     // for now, just null out the entity
        //     // self._entities[ entity.id ] = null;
        //     return callback(null, entity);
        // });    
    },


    /**
     * Returns an array of entities which have the specified
     * Component Defs
     *
     * Callback returns error, entities, componentDefs
     * 
     * @param  {[type]}   defs     [description]
     * @param  {Function} callback [description]
     * @return {[type]}            [description]
     */
    getEntitiesWithComponents: function( componentDefs, options, callback ){
        var self = this,
            result = {};
        var registry = self.registry;
        var returnObjects = true;

        var startIndex = _.isUndefined(options.start) ? 0 : options.start;
        var limit = _.isUndefined(options.limit) ?  50 : options.limit;
        var selectOptions = {columns:['id','entity_id']};

        async.eachSeries( componentDefs, 
            function(componentDef, componentDefCallback){
                var query = Schema.toSelect( componentDef.schema.id, selectOptions );
                return self.allRows( query.get('sql'), null, function(err, rows){
                    if( err ){
                        log.error('error getEntitiesWithComponents ' + err );
                        return callback(err);
                    }
                    var entities = rows.map( function(row){
                        result[row.entity_id] = odgnEntity.Entity.toEntity( row.entity_id );
                    });
                    return componentDefCallback();
                });
            },
            function(err){
                result = _.values(result);
                return callback( null, result, componentDefs);
            });
    },

    /**
     * 
     * @param  {[type]}   componentDef [description]
     * @param  {[type]}   attrs        [description]
     * @param  {[type]}   options      [description]
     * @param  {Function} callback     [description]
     * @return {[type]}                [description]
     */
    createComponent: function( componentDef, attrs, options, callback ){
        var self = this;

        // apply default schema attributes and passed attributes to the component
        var properties = _.extend( {}, odgnEntity.Schema.getDefaultValues( componentDef.schema.id ), attrs );

        // instantiate a component instance from the supplied data - 
        // this is commonly used when recreating from a serialised form
        
        if( options && options.instantiate ){
            var component = componentDef.create(properties, options);
            component.defId = componentDef.id;
            component.schemaId = componentDef.schema.id;
            component.registry = self.registry;

            if( callback ){
                return callback(null, component);
            }

            return component;
        }

        var query = Schema.toInsert( [componentDef.schema.id, '/schema/component_defaults'], {isComponent:true, debug:false} );

        return self.execQuery( query, {}, function(err, lastId){
            if( err ){ log.error('error inserting component: ' + err); print_ins(err.errno); return callback(err); }

            var component = componentDef.create(properties, options);
            component.id = lastId;
            component.defId = componentDef.id;
            component.schemaId = componentDef.schema.id;
            component.registry = self.registry;

            log.debug('inserted new ' + componentDef.schema.id + ' id ' + component.id );

            return callback(null, component);
        });
    },


    /**
     * Adds a component to an entity
     * 
     * @param  {[type]}   component [description]
     * @param  {[type]}   entity    [description]
     * @param  {Function} callback  [description]
     * @return {[type]}             [description]
     */
    addComponent: function(component, entity, callback ){
        var self = this;
        // update the component records entity_id
        var query = Schema.toUpdate( [component.schemaId, '/schema/component_defaults'], {entity_id:entity.id}, {debug:true} );
        var values = [ entity.id, component.id ];

        // log.debug( query.get('sql') + ' ' + JSON.stringify(values) );

        return self.execQuery( query, {values:values}, function(err, lastId){
            if( err ){ log.error('error updating component: ' + err); print_ins(err.errno); return callback(err); }

            // update the entities component bitmask (stores the def)
            entity.get('component_bf').set( component.defId, true );

            query = Schema.toUpdate( '/schema/entity', {component_bf:true} );
            var values = [ entity.get('component_bf').toHexString(), entity.id ];

            return self.execQuery( query, {values:values}, function(err, lastId){
                if( err ){ log.error('error updating entity: ' + err); print_ins(err.errno); return callback(err); }

                self.trigger('component:add', component, entity, {} );
                return callback( null, component, entity );
            });
        });
    },

    /**
     * Returns a component for an entity
     * 
     * @param  {[type]}   component [description]
     * @param  {[type]}   entity    [description]
     * @param  {Function} callback  [description]
     * @return {[type]}             [description]
     */
    getComponentForEntity:function( componentDef, entity, callback ){
        var self = this;
        // log.debug('ms getComponentForEntity ' + componentDef.id + ' ' + entity.id );

        var query = Schema.toSelect( componentDef.schema.id, {where:{id:entity.id}} );
        // var values = [ entity.id ];

        // log.debug( query.get('sql') );

        return self.allRows( query.get('sql'), query.get('parameters'), function(err, rows){
            if( err ){
                // log.error('error getComponentForEntity ' + err );
                return callback(err);
            }
            var options = { instantiate:true };
            var components = rows.map( function(row){
                return self.createComponent( componentDef, row, options );
            });
            
            if( components.length == 1 )
                return callback( null, components[0] );
            return callback( null, components );
        });
    },


    /**
     * Removes a component from an entity
     * @param  {[type]}   component [description]
     * @param  {[type]}   entity    [description]
     * @param  {Function} callback  [description]
     * @return {[type]}             [description]
     */
    removeComponent: function( component, entity, callback ){
        var self = this;

        throw new Error('not yet implemented');

        /*var componentsByEntity = this._componentsByType[ component.defId ];
        // log.debug('removing component ' + component.defId + ' from ' + entity.id );

        if( componentsByEntity ){
            delete componentsByEntity[ entity.id ];
        }

        component.entityId = null;

        return async.nextTick( function(){
            self.trigger('component:remove', component, entity, {} );
            return callback( null, component, entity );
        });//*/
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
            // log.debug( '_dCTE ' + componentDef.id + ' ' + JSON.stringify(arguments) );
            // log.debug('_doesComponentTableExist ' + stmt.get('sql') + ' ' + row );
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
        // log.debug('!!! ensureComponentTables');
        this._eachComponentDef({}, function(err, componentDef, rowCb){
            // log.debug('+++ checking com table ' + componentDef.id );

            // print_ins(arguments);
            // 
            return self._doesComponentTableExist( componentDef, function(err, exists){
                // log.debug('??? dCTE result ' + exists );
                if( err ){ log.error('error checking com table ' + err ); }
                if( !exists ){
                    // create the table
                    var query = Schema.toCreate( [componentDef, '/schema/component_defaults'], {isComponent:true} );
                    // log.debug('creating com table ' + query.get('table_name') );
                    // log.debug( query.get('sql') );
                    self.execQuery( query, null, function(err){
                        // process.exit();
                        return rowCb();
                    });
                } else {
                    // log.debug( componentDef.id + ' exists');
                    return rowCb();
                }
            });

        }, function(err){
            if( err ){ log.error( "something went wrong? " + err ) };
            // log.debug('!!! finished ensureComponentTables');
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
            var componentDef = JSON.parse(row.schema);
            // log.debug( '@@@ ' + componentDef.id );
            return componentDefCallback( null, componentDef, function(){
                // log.debug('£££ finished ' + componentDef.id )
                return cb(); 
            });
        }, callback );
    },


    /**
    *   Override
    */
    getComponentDef: function( defId, callback ){
        return this.getComponentDefByUri( defId, callback );
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
        log.debug('registering com ' + componentDef.schema.id );

        return this.getComponentDefByUri( componentDef.schema.id, function(err, def){
            if( def ){
                log.debug('def already registered ' + def.id );
                return callback( null, componentDef );
            }
            // log.debug('def not registered ' + componentDef.schema.id );
            // log.debug('getComponentDefByUri returned ' + JSON.stringify(def) );
            // create a table
            
            // process.exit();

            // insert the def in the component_def table
            var query = Schema.toInsert( '/schema/component_def' );

            query.set('uri', componentDef.schema.id );
            query.set('schema', JSON.stringify(componentDef.schema) );

            return self.execQuery( query, null, function(err, lastId){
                if( err ){ log.error('error inserting component_def: ' + err); print_ins(err.errno); return callback(err); }
                componentDef.id = componentDef.defId = lastId;

                // record the (string) schema id against the (int) component def id
                // this is largely a static store, and so OK to store in memory
                self.componentDefSchemaIds[ componentDef.schema.id ] = componentDef.id;
                self.componentDefIds[ componentDef.id ] = componentDef.schema.id;

                log.debug('inserted new comDef id ' + componentDef.id );

                // we now ensure that all defined components have a table instantiated
                return self.ensureComponentTables( options, function(err){
                    if(err){ log.warn('error ensuring tables ' + err ); }
                    // log.debug('B. finished ensuring component tables');
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
    execQuery: function( query, options, callback ){
        options = options || {};
        var values = options.values || query.getValues();
        return this.dbHandle.run( query.get('sql'), values, function(err){
            if( options && options.debug) log.debug('execQuery ' + query.get('sql'));
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

    /**
     * Runs the SQL query with the specified params and calls
     * the callback with all result rows
     * 
     * @param  {[type]}   sql      [description]
     * @param  {[type]}   params   [description]
     * @param  {Function} callback [description]
     * @return {[type]}            [description]
     */
    allRows: function( sql, params, callback ){
        params = params || [];
        return this.dbHandle.all( sql, params, callback );
    },

    eachRow: function( sql, params, rowCallback, callback ){
        params = params || [];

        // the sqlite driver doesn't wait for row callbacks to
        // finish before calling the complete callback - so we set
        // up a queue which will ensure that this happens
        var q = async.queue( function(row, taskCb){
            return rowCallback( null, row, taskCb );
        },1);

        q.drain = function(){
            // completed
            // log.debug('finished eachRow queue');
            return callback();
        };

        var rowCb = function(err,row){
            q.push( row );
        };

        return this.dbHandle.each( sql, params, rowCb, function(err,rowCount){
            // log.debug(rowCount + ' rows retrieved');
        });
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
                // log.debug('profile: ' + sql + ' ' + time);
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