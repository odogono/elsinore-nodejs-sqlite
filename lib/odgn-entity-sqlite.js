var async = require('async');
var fs = require('fs');
var sqlite3 = require('sqlite3').verbose();
var Schema = require('./schema');
var odgnEntitySchema = odgnEntity.Schema;
var Entity = odgnEntity.Entity;
var Component = odgnEntity.Component;
Schema.setup( odgnEntity.Schema );

module.exports = {
    create:function(registry, options){
        options = options || {};
        var result = new SqliteStorage(options);
        result.registry = registry;
        return result;
    },

    loadSql: function( filename, sql, callback ){
        var result = new SqliteStorage({filename:filename});
        return result._open( function(db){
            result.exec( sql, function(err){
                return callback( err, result );
            });
        });
    }
};

var SqliteStorage = function(options){
    this.filename = options.filename || ':memory:';
    this.isNew = false;
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
                if( options.clearAll ){
                    self.isNew = true;
                    return self._clearTables(null, cb);
                }
                return cb();
                // return (options.clearAll) ? self._clearTables(null,cb) : cb();
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
                    // log.debug(statement.getSql());
                    self.rawQuery(statement.getSql(),function(err){
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
            function loadComponentDefs(cb){
                self.retrieveComponentDefs(null, cb);
            }
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

        // print_ins( entity.toJSON() ); process.exit();
        var query = Schema.toInsert( '/schema/entity', entity.toJSON() );
        query.setValues( entity.toJSON() );
        // log.debug('createEntity ' + query.getSql() );
        // print_ins( query.getValues(true) );
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
     * Returns an entity from the db
     * 
     * @param  {[type]}   entity   [description]
     * @param  {[type]}   options  [description]
     * @param  {Function} callback [description]
     * @return {[type]}            [description]
     */
    getEntity: function(entity, options, callback){
        entity = Entity.toEntity(entity);

        return this.getRow( "SELECT * FROM tbl_entity WHERE id = ?", entity.id, function(err,row){
            if( err ){ log.debug('getEntity ' + err ); return callback(err); }
            if( !row ){
                return callback();
            }
            entity.set( entity.parse(row) );
            return callback( null, entity );
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
                return self.allRows( query.getSql(), null, function(err, rows){
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

    _toComponentDef: function( componentDef ){
        if( _.isInteger(componentDef) )
            return this.componentDefIds[ componentDef ];
        if( _.isString(componentDef) )
            return this.componentDefSchemaIds[ componentDef ];
        return componentDef;
    },


    retrieveComponent: function( componentDefOrSchemaId, options, callback ){
        var self = this, componentDef = this._toComponentDef(componentDefOrSchemaId);
        var stmt = Schema.toSelect( componentDef.schema.id, options );

        return self.getRow( stmt, null, function(err,row){
            if( err ){ log.debug('rC error ' + err ); return callback(err); }
            var component, entity;
            if( row ){
                component = self.createComponent( componentDef, row, _.extend({instantiate:true},options));
                if( row.entity_id ){
                    entity = Entity.toEntity( row.entity_id );
                    entity.registry = self.registry;
                }
            }
            // var component = row ? self.createComponent( componentDef, row, _.extend({instantiate:true},options)) : null;
            // pass instantiate to only create a component instance
            return callback( null, component, entity );
        });
    },

    
    /**
     * Retrieves a list of components
     * 
     * @param  {[type]} componentDefArray [description]
     * @param  {[type]} componentCallback [description]
     * @param  {[type]} completeCallback  [description]
     * @return {[type]}                   [description]
     */
    retrieveComponents: function( componentDefArray, options, componentCallback, completeCallback ){
        var self = this;
        // return async.nextTick( function(){
        //     if( !componentDefArray ){
        //         _.each( self._components, componentCallback );
        //         return completeCallback();
        //     }
        // });

        componentDefArray = Array.isArray(componentDefArray) ? componentDefArray : [ componentDefArray ];

        return async.eachSeries( componentDefArray, 
            function(componentDef,componentDefCallback){

                var query = Schema.toSelect( componentDef.schema.id );

                return this.eachRow( query.getSql(), null, function(err, row, cb){
                    if( err ){ log.debug('error with ' + err ); return callback(err); }
                    var component = JSON.parse(row.schema);
                    
                    return componentCallback( null, componentDef, function(){
                        return cb();
                    });
                    }, callback );
            },
            function(err){
                if( err ){ return callback(err); }
                return callback( null, result );
            });
    },


    /**
     * Returns all of an entities components
     * 
     * @param  {[type]}   entity   [description]
     * @param  {[type]}   options  [description]
     * @param  {Function} callback [description]
     * @return {[type]}            [description]
     */
    getEntityComponents: function( entity, options, callback ){
        var self = this;
        var result = [];

        return this.getEntity( entity, options, function(err,entity){
            var bf = entity.get('component_bf');
            var componentDefIds = [];

            // build an array of schema ids for this entity
            for( var cdId in self.componentDefIds ){
                if( bf.get(cdId) ){
                    componentDefIds.push( self.componentDefIds[cdId] );
                }
            }

            return async.eachSeries( componentDefIds, 
                function(cDef,cDefIdCb){
                    self.getComponentForEntity( cDef, entity, function(err,components){
                        if( Array.isArray(components) ){
                            // add the components to the end of the results
                            result.push.apply( result, components ); // faster
                            // result = result.concat( components ); // clearer
                        } else {
                            result.push( components );
                        }
                        return cDefIdCb(err);
                    })
                },
                function(err){
                    if( err ){ return callback(err); }
                    return callback( null, result );
                });

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

        // log.debug('createComponent:');
        // print_ins(arguments);
        // var schemaId = Component.isComponentDef(componentDef) ? componentDef.schema.id : componentDef;
        // apply default schema attributes and passed attributes to the component
        var defaultProperties = odgnEntity.Schema.getDefaultValues( componentDef.schema.id );
        // var properties = _.extend( {}, odgnEntity.Schema.getDefaultValues( componentDef.schema.id ), attrs );

        var componentCreate = function( componentAttrs, options ){
            var component = componentDef.create(componentAttrs, options);
            component.defId = componentDef.id;
            component.schemaId = componentDef.schema.id;
            component.registry = self.registry;

            return component;
        };

        // instantiate a component instance from the supplied data - 
        // this is commonly used when recreating from a serialised form
        if( options && options.instantiate ){

            attrs = _.isArray(attrs) ? attrs : [ attrs ];
            var components = _.map( attrs, function( att ){
                // TODO : refine this
                // additional properties are serialised as JSON
                if( att._additional )
                    att._additional = JSON.parse(att._additional);
                var properties = _.extend( {}, defaultProperties, att, att._additional );
                return componentCreate( properties, options );
            });

            if( components.length == 1 ){
                components = components[0];
            }

            if( callback ){
                return callback(null, components, componentDef);
            }

            return components;
        }

        var query = Schema.toInsert( [componentDef.schema.id, '/schema/component_defaults'], 
            null, 
            {isComponent:true, debug:false} );

        if( _.isArray(attrs) ){
            async.mapSeries( attrs,
                function( componentAttrs, componentAttrsCallback ){
                    var properties = _.extend( {}, defaultProperties, componentAttrs );
                    query.setValues( properties );

                    // log.debug( query.get('names') );
                    // log.debug( query.getValues() );
                    // process.exit();

                    return self.execQuery( query, {debug:true}, function(err, lastId){
                        if( err ){ return componentAttrsCallback(err); }
                        var component = componentCreate( properties, options ); 
                        component.id = lastId;
                        log.debug('A inserted new ' + componentDef.schema.id + ' id ' + component.id );
                        return componentAttrsCallback(null, component);
                    });
                },
                function(err, results){
                    return callback(err, results);
                });
        }
        else {
            var properties = _.extend( {}, defaultProperties, attrs );
            query.setValues( properties );

            return self.execQuery( query, {}, function(err, lastId){
                if( err ){ log.error('error inserting component: ' + err); print_ins(err.errno); return callback(err); }

                var component = componentCreate( properties, options ); 
                component.id = lastId;
                log.debug('B inserted new ' + componentDef.schema.id + ' id ' + component.id );

                return callback(null, component);
            });
        }
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

        // log.debug( query.getSql() + ' ' + JSON.stringify(values) );

        return self.execQuery( query, {values:values}, function(err, lastId){
            if( err ){ log.error('error updating component: ' + err); print_ins(err.errno); return callback(err); }

            // update the entities component bitmask (stores the def)
            
            entity.get('component_bf').set( component.defId, true );
            // log.debug('entity ' + entity.id + ' ' + entity.cid + ' ' + entity.get('component_bf') );

            query = Schema.toUpdate( '/schema/entity', {component_bf:true} );
            var values = [ entity.get('component_bf').toHexString(), entity.id ];

            // log.debug( query.getSql() + ' ' + JSON.stringify(values) );

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
        // var schemaId = Component.isComponentDef(componentDef) ? componentDef.schema.id : componentDef;
        log.debug('ms getComponentForEntity ' + componentDef.id + ' ' + entity.id );

        var query = Schema.toSelect( componentDef.schema.id, {where:{id:entity.id}} );
        
        return self.allRows( query.getSql(), query.getParameters(), function(err, rows){
            if( err ){
                // log.error('error getComponentForEntity ' + err );
                return callback(err);
            }
            var options = { instantiate:true };
            var components = rows.map( function(row){
                return self.createComponent( componentDef, row, options );
            });
            
            if( components.length == 1 )
                return callback( null, components[0], entity );
            return callback( null, components, entity );
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
        return this.getRow( stmt.getSql(), null, function(err,row){
            // log.debug( '_dCTE ' + componentDef.id + ' ' + JSON.stringify(arguments) );
            // log.debug('_doesComponentTableExist ' + stmt.getSql() + ' ' + row );
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
                    // log.debug('creating com table ' + query.getTableName() );
                    // log.debug( query.getSql() );
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
        log.debug('sqlite:registerComponent: ' + componentDef.schema.id );

        if( !callback ){
            self.componentDefSchemaIds[ componentDef.schema.id ] = componentDef;
            self.componentDefIds[ componentDef.id ] = componentDef;
            return componentDef;
        }

        return this.getComponentDefByUri( componentDef.schema.id, function(err, def){
            if( def ){
                log.debug('def already registered ' + def.id );
                
                componentDef.id = def.id;
                // record the (string) schema id against the (int) component def id
                // this is largely a static store, and so OK to store in memory
                self.componentDefSchemaIds[ componentDef.schema.id ] = componentDef; //.id;
                self.componentDefIds[ componentDef.id ] = componentDef;

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
                self.componentDefSchemaIds[ componentDef.schema.id ] = componentDef; //.id;
                self.componentDefIds[ componentDef.id ] = componentDef;

                log.debug('inserted new comDef id ' + componentDef.id );

                // we now ensure that all defined components have a table instantiated
                return self.ensureComponentTables( options, function(err){
                    if(err){ log.warn('error ensuring tables ' + err ); }
                    // log.debug('B. finished ensuring component tables');
                    return callback(null,componentDef);    
                });
            });
        });
    },

    /**
     * 
     * @param  {[type]}   options  [description]
     * @param  {Function} callback [description]
     * @return {[type]}            [description]
     */
    retrieveComponentDefs: function( options, callback ){
        var self = this;
        var sql = 'SELECT * FROM tbl_component_def';

        return self.allRows( sql, null, function(err, rows){
            if( err ){
                log.error('error retrieveComponentDefs ' + err );
                return callback(err);
            }
            
            var componentSchemas = rows.map( function(row){
                return {id:row.id, schema:JSON.parse(row.schema) };
            });

            var componentDefs = self.registry.registerComponent( componentSchemas, {persist:false} );

            return callback(null, componentDefs);
        });
    },


    /**
     * Creates a new entityset
     * 
     * @param  {[type]}   options  [description]
     * @param  {Function} callback [description]
     * @return {[type]}            [description]
     */
    createEntitySet: function( options, callback ){
        var self = this;
        options = options || {};
        
        var result = odgnEntity.EntitySet.create( this, this.registry, options );

        return result.reload( function(err,pEntitySet){
            self.trigger('entity_set:create', pEntitySet );
            return callback(null, pEntitySet);
        });
    },


    retrieve: function( query, callback ){
        var sql = _.isString(query) ? query : query.getSql();
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
        var values = options.values || query.getValues(options.debug);
        // if( options.debug ){
            // log.debug( query.getSql() + ' ' + JSON.stringify(values) );
            // log.debug( JSON.stringify(values) );
        //     print_ins( query ); process.exit();
        // }
        return this.dbHandle.run( query.getSql(), values, function(err){
            // if( options && options.debug) 
                // log.debug('execQuery ' + query.getSql());
            if( err ){ log.debug('execQuery err ' + err ); return callback(err); }
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

    exec: function( sql, callback ){
        return this.dbHandle.exec( sql, function(err){
            if( err ){ return callback(err); }
            return callback();
        });
    },

    getRow: function( sql, params, callback ){
        sql = this._getSql(sql);
        params = params || [];
        return this.dbHandle.get( sql, params, callback );
    },

    _getSql: function( sql ){
        return Schema.SqlStatement.isSqlStatement(sql) ? sql.getSql() : sql;
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
        if( !fs.existsSync( this.filename) ){
            self.isNew = true;
        }
        return this.dbHandle = new sqlite3.cached.Database( this.filename, function(err){
            if( err ){ log.error( err ); return callback(err); }
            log.debug('opened db ' + self.filename );
            // self.dbHandle.on('profile', function(sql,time){
            //     log.debug('profile: (' + time + ') ' + sql);
            // });
            return callback(null, self.dbHandle);
        });
    },

    _close: function(callback){
        this.dbHandle.close();
        this.dbHandle = null;
        return null;
    }

});