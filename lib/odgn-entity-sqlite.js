var async = require('async');
var fs = require('fs');
var sqlite3 = require('sqlite3');
var Schema = require('./schema');
var odgnEntitySchema = odgnEntity.Schema;
var Entity = odgnEntity.Entity;
var Component = odgnEntity.Component;
var EntitySet = require('./entity_set');
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
    }//,

    // dumpDatabase: function( dbFilePath, sqlFilePath, callback ){
    //     var storage = new SqliteStorage({filename:dbFilePath});
    //     return storage._open( function(db){

    //     });
    // }
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

        if( options.verbose ){
            log.info('enabling sqlite3 verbose mode');
            sqlite3.verbose();
        }

        
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
                return self.dbHandle.serialize( function(){
                    // queue an operation for creating tables from the
                    // default schemas
                    log.info('registering default schemas');
                    var q = async.queue(function(schema,qCb){
                        log.info('registering schema ' + schema.id );
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

                    _.each( Schema.schemaTables, q.push );

                    // called when finished
                    q.drain = function(){
                        return cb();
                    };
                });
            },
            function loadComponentDefs(cb){
                return self.dbHandle.serialize( function(){
                    return self.retrieveComponentDefs(null, cb);
                });
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

        var query = Schema.toInsert( '/schema/entity', entity.toJSON() );
        query.setValues( entity.toJSON() );
        // log.debug('createEntity ' + query.getSql() );
        // print_ins( query.getValues(true) );
        // print_ins( entity.toJSON() ) ;
        // print_ins( entity );
        


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
        entity = this.registry.toEntity(entity);
        // if( !entity ){
        //     return callback('no entity');
        // }

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

    /**
     * 
     * @param  {[type]}   entity   [description]
     * @param  {[type]}   options  [description]
     * @param  {Function} callback [description]
     * @return {[type]}            [description]
     */
    destroyEntity: function(entity, options, callback ){
        var self = this;
        
        entity = Entity.toEntity(entity);

        return this.execQuery('DELETE FROM tbl_entity WHERE id=?', {values:[entity.id]}, function(err){
            return callback(err,entity);
        });   
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


    retrieveComponentById: function( componentDefOrSchemaId, componentId, options, callback ){
        var self = this, componentDef = this._toComponentDef(componentDefOrSchemaId);
        var stmt = Schema.toSelect( componentDef.schema.id, _.extend( {},options, {where:{id:componentId},limit:1} ) );

        log.debug('retrieveComponentById: ' + stmt.getSql());
        return self.getRow( stmt, [componentId], function(err,row){
            if( err ){ log.debug('rC error ' + err ); return callback(err); }
            if( !row ){
                return callback();
            }
            component = self.createComponent( componentDef, row, _.extend({instantiate:true},options));
            return callback(null,component);
        });
    },


    /**
     * Updates the fields of a component in the database
     * 
     * @param  {[type]}   components [description]
     * @param  {[type]}   options    [description]
     * @param  {Function} callback   [description]
     * @return {[type]}              [description]
     */
    updateComponent: function( components, entity, options, callback ){
        var self = this;
        options = options || {};
        var debug = options.debug;
        var isSingle = components.length == 1;
        var first = components[0];
        var schemaId = first.schemaId;

        var schemas = [schemaId, '/schema/component_defaults'];
        var coSchema = odgnEntitySchema.getSchema( schemas, {combine:true} );
        var primaryKey = coSchema.primaryKey;
        var propertyDetails = odgnEntity.Schema.getProperties( schemas );

        var attr, columnNames = [], columnTypes = [];

        for( var i in propertyDetails ){
            attr = propertyDetails[i];
            if( attr.name == primaryKey )
                continue;
            columnNames.push( attr.name );
            columnTypes.push( attr.type == 'string' ? attr.format : attr.type );
        }

        var queryInsert = Schema.toInsert( schemas, null,  {isComponent:true, debug:false} );
        var query = Schema.toUpdate( schemas, columnNames, {debug:false} );
        var sql = query.getSql();
        var updateStmt = this.dbHandle.prepare( query.getSql() );
        var currentDate = new Date().toISOString();
        // print_ins( propertyDetails );
        // print_ins( query.getSql() );

        var runCallback = function(err){
            if( err ){
                log.debug('error running update stmt ' + err );
            }
        };

        return this.dbHandle.serialize( function(){

            return async.mapSeries( components,
                function eachComponent(component,componentCallback){
                    if( !component.id ){
                        if( debug ){ log.debug('no component.id found for component ' + component.schemaId + ' ' + component.toJSON() ); }
                        return self.getEntity( entity, null, function(err,entity){
                            if( err ){ log.warn('error retrieving entity for component ' + component.schemaId ); }
                            return entity.addComponent( _.extend(component.toJSON(), {schemaId:component.schemaId}), componentCallback );
                        });
                    }

                    var attr, columnName, columnType, values = [];

                    for( var i in columnNames ){
                        columnName = columnNames[i];
                        columnType = columnTypes[i];
                        if( columnName == 'entity_id' ){
                            val = component.entityId;
                        } else {
                            val = component.get( columnName );
                            if( columnName == '_updated_at' )
                                val = currentDate;
                            else if( columnType == 'json' )
                                val = JSON.stringify(val);
                        }
                        values.push( val );
                    }
                    values.push( component.id );
                    
                    // log.debug('update with ' + JSON.stringify(values) );
                    // updateStmt.run.apply( null, values );
                    self.dbHandle.run( sql, values, function(err){
                        if( err ){
                            log.debug('error running update stmt ' + err );
                        }

                        return componentCallback(null, component );
                    });
                },
                function(err, components){
                    if( isSingle ){
                        components = components[0];
                    }
                    return callback(null, components );
                });

            // _.each( components, function(component){
                
            });
        
        // if( isSingle ){
        //     components = components[0];
        // }
        // return callback(null, components );
    },

    /**
     * [ description]
     * @param  {[type]}   componentDefOrSchemaId [description]
     * @param  {[type]}   options                [description]
     * @param  {Function} callback               [description]
     * @return {[type]}                          [description]
     */
    retrieveComponent: function( componentDefOrSchemaId, options, callback ){
        var self = this, 
            componentDef = this._toComponentDef(componentDefOrSchemaId);
        options = options || {};
        if( !componentDef ){
            log.error('no component found for ' + componentDefOrSchemaId );
            return callback('no component found for ' + componentDefOrSchemaId );
        }
        if( options.select ){
            return self.retrieveComponentEx( options, callback );    
        }
        var debug = options.debug;
        var schemaId = componentDef.schema.id;
        // print_ins( componentDefOrSchemaId );
        var stmt = Schema.toSelect( schemaId, _.extend( {},options, {includeEntity:true} ) );

        if( debug ){ log.debug('retrieveComponent: ' + stmt.getSql()); }

        return self.getRow( stmt, null, function(err,row){
            if( err ){ log.debug('rC error ' + err ); return callback(err); }
            if( !row ){
                return callback();
            }
            var component, entity, entityCBF;

            // capture and remove the entity component bitfield before it gets set on the component
            if( row.entity_component_bf ){
                entityCBF = String(row.entity_component_bf);
                delete row.entity_component_bf;
            }
            component = self.createComponent( componentDef, row, _.extend({instantiate:true},options));
            if( row.entity_id ){
                entity = Entity.toEntity( row.entity_id );
                entity.registry = self.registry;
                entity.set( entity.parse({component_bf:entityCBF}) );
            }

            if( entity && options.decorate ){
                componentName = odgnEntity.Schema.titleFromSchema(schemaId);
                entity[ componentName ] = component;
            }
            // var component = row ? self.createComponent( componentDef, row, _.extend({instantiate:true},options)) : null;
            // pass instantiate to only create a component instance
            return callback( null, component, entity );
        });
    },


    retrieveComponentEx: function( options, callback ){
        var self = this;
        var statement = Schema.toComponentSelect( options.select, {includeEntity:true} );
        var tables = statement.get('_tables');

        var sql = statement.getSql();
        sql = sql + ' LIMIT 1';

        var entityId;

        return self.getRow( statement, null, function(err,row){
            if( err ){ log.debug('rC error ' + err ); return callback(err); }
            if( !row ){
                return callback();
            }

            if( !row.entity_id ){
                return callback('entity not found');
            }
            
            
            var entity = self.registry.toEntity( row.entity_id );

            // capture and remove the entity component bitfield before it gets set on the component
            if( row.entity_component_bf ){
                entityCBF = String(row.entity_component_bf);
                delete row.entity_component_bf;
                entity.set( entity.parse({component_bf:entityCBF}) );
            }
            
            var components = [];
            var createComponentOptions = { instantiate:true, debug:true };
            _.each( tables, function(table){
                var tablePrefix = table.hashName + '_';
                var tablePrefixLen = tablePrefix.length;
                var componentData = { schemaId:table.schemaId };
                var attrName;
                for( var c in row ){
                    if( c.indexOf(tablePrefix) == 0 ){
                        attrName = c.substring(tablePrefixLen)
                        if( attrName == 'entity_id' ) {//&& !entityId ){
                            // we are returning only components which are directly related to the entity
                            if( row[c] != entity.id ){
                                return;
                            }
                            // entityId = row[c];
                        }
                        else if( attrName == '_additional' ){
                            componentData = _.extend( componentData, JSON.parse( row[c] ) );
                        }
                        else
                            componentData[ attrName ] = row[c];
                    }
                }

                if( componentData.id == null ){
                    // log.debug('not adding component ' + table.schemaId + ' ' + componentData.id + ' for entity ' + entityId ); 
                    return;
                }
                
                var component = self.registry.createComponent( componentData, null, createComponentOptions );
                components.push( component );
                // log.debug('created component ' + table.schemaId + ' ' + componentData.id + ' for entity ' + entityId );

                // var componentName = ; // odgnEntity.Schema.titleFromSchema(schemaId);
                entity[ table.name ] = component;
            });

            return callback( null, components, entity );
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
        var resultComponentArray = [];
        options = options || {};

        var processEntity = function(err,pEntity){
             if( options.debug ){ print_ins( arguments ); log.debug('getEntityComponents: ' + JSON.stringify(pEntity) );}
             if( !pEntity ){
                return callback();
             }

             var bf = pEntity.get('component_bf');
             var componentDefIds = [];

             // build an array of schema ids for this pEntity
             for( var cdId in self.componentDefIds ){
                 if( bf.get(cdId) ){
                    if( options.debug ) log.debug('adding com ' + self.componentDefIds[cdId].schema.id );
                     componentDefIds.push( self.componentDefIds[cdId] );
                 }
             }

             if( componentDefIds.length <= 0 ){
                 if( options.debug ) log.debug('getEntityComponents: no components found for entity ' + pEntity.id );
                 return callback( null, [], pEntity );
             }

            return async.eachSeries( componentDefIds, 
                function(cDef,cDefIdCb){
                    return self.getComponentForEntity( cDef, pEntity, null, function(err,components){
                        var componentName;
                        if( options.decorate ){
                            componentName = odgnEntity.Schema.titleFromSchema(cDef.schema.id);
                        }
                        if( Array.isArray(components) ){
                            // add the components to the end of the resultComponentArray
                            resultComponentArray.push.apply( resultComponentArray, components ); // faster
                            // resultComponentArray = resultComponentArray.concat( components ); // clearer
                        } else {
                            resultComponentArray.push( components );
                        }
                        if( options.decorate ){
                            pEntity[ componentName ] = components;
                        }
                        return cDefIdCb(err);
                    });
                },
                function(err){
                    if( err ){ return callback(err); }
                    return callback( null, resultComponentArray, pEntity );
                });
        };

        // if we have already been passed a valid entity then just use it
        if( options.useEntity ){
            return processEntity(null, entity );
        }

        // retrieve the entity before processing
        return this.getEntity( entity, options, processEntity );
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

        // log.debug('*** createComponent:');
        // log.debug( odgnEntity.Schema.getProperties( componentDef.schema.id, {debug:true,names:true} ) );

        // print_ins(arguments);
        // var schemaId = Component.isComponentDef(componentDef) ? componentDef.schema.id : componentDef;
        // apply default schema attributes and passed attributes to the component
        var defaultProperties = odgnEntity.Schema.getDefaultValues( componentDef.schema.id );
        var propertyDetails = odgnEntity.Schema.getPropertyDetails( [componentDef.schema.id, '/schema/component_defaults'] );

        var componentCreate = function( componentAttrs, options ){
            
            var component = componentDef.create(componentAttrs, options);
            // if( options.debug ) print_ins( component ); //log.debug('here ' + JSON.stringify(component) );
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
                for( var k in att ){
                    var details = propertyDetails[k];
                    // console.log('hmm ' + JSON.stringify(att[k]) );
                    if( !details ){
                        continue;
                        // log.debug('no details found for ' + k + ' ' + JSON.stringify(att[k]) );
                    }
                    if( _.isString(att[k]) && details.type == 'string' && details.format == 'json' ){
                        att[k] = JSON.parse( att[k] );
                    }
                }

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
        // print_ins( query );

        if( _.isArray(attrs) ){
            async.mapSeries( attrs,
                function( componentAttrs, componentAttrsCallback ){
                    var properties = _.extend( {}, defaultProperties, componentAttrs );
                    query.setValues( properties );

                    // log.debug( query.get('names') );
                    // log.debug( query.getValues() );
                    

                    return self.execQuery( query, {debug:false}, function(err, lastId){
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

            log.debug('inserting new ' + componentDef.schema.id + ' ' + JSON.stringify(defaultProperties));
            // log.debug('inserting new ' + componentDef.schema.id + ' ' + JSON.stringify(odgnEntity.Schema.getDefaultValues( componentDef.schema.id )));
            
            query.setValues( properties );
            // print_ins( query );

            return self.execQuery( query, {debug:true}, function(err, lastId){
                if( err ){ log.error('error (' + err.errno + ') inserting component ' + componentDef.schema.id + ' : ' + err); log.error( query.getSql() ); return callback(err); }

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
        var query = Schema.toUpdate( [component.schemaId, '/schema/component_defaults'], {entity_id:entity.id}, {debug:false} );
        var values = [ entity.id, component.id ];

        // log.debug( query.getSql() + ' ' + JSON.stringify(values) );

        return self.execQuery( query, {values:values}, function(err, lastId){
            if( err ){ log.error('error updating component: ' + err); print_ins(err.errno); return callback(err); }

            component.entityId = entity.id;

            // update the entities component bitmask (stores the def)
            
            entity.get('component_bf').set( component.defId, true );
            // log.debug('entity ' + entity.id + ' ' + entity.cid + ' ' + entity.get('component_bf') );

            query = Schema.toUpdate( '/schema/entity', {component_bf:true} );
            var values = [ '0x' + entity.get('component_bf').toHexString(), entity.id ];

            // if( entity.id == 8 ) {
            //     log.debug( ticket + ' *** ' + query.getSql() + ' ' + JSON.stringify(values) );
            //     // query = "UPDATE tbl_entity SET component_bf ='" + entity.get('component_bf').toHexString() + "' WHERE id=" + entity.id;
            // }

            return self.execQuery( query, {values:values}, function(err, lastId){
                if( err ){ log.error('error updating entity: ' + err); print_ins(err.errno); return callback(err); }
                self.trigger('component:add', component, entity, {} );
                // if( entity.id == 8 ){
                //     log.debug(ticket + ' CCC addComponent for ' + entity.id + ' ' + lastId);
                //     return self.dbHandle.get( 'SELECT id,component_bf FROM tbl_entity WHERE id=8', {}, function(err,row){
                //         print_ins( ticket + ' ' + JSON.stringify(arguments) + ' ' + entity.get('component_bf').toHexString());
                //         return callback( null, component, entity );
                //     });
                // }
                return callback( null, component, entity );
            });
        });
    },


    saveEntity: function( entity, options, callback ){
        var self = this,
            query = Schema.toUpdate( '/schema/entity', {component_bf:true} );
        var values = [ '0x' + entity.get('component_bf').toHexString(), entity.id ];

        return self.execQuery( query, {values:values}, function(err, lastId){
            if( err ){ log.error('error updating entity: ' + err); print_ins(err.errno); return callback(err); }
            return callback(null, entity);
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
    getComponentForEntity:function( componentDef, entity, options, callback ){
        var self = this;
        options = options || {};
        var debug = options.debug;
        if( !entity ){
            log.debug('no entity passed');
        }
        if( !componentDef ){
            log.debug('no componentDef passed');
        }
        // var schemaId = Component.isComponentDef(componentDef) ? componentDef.schema.id : componentDef;
        // 

        var query = Schema.toSelect( componentDef.schema.id, {where:{entity_id:entity.id}, limit:1} );
        
        if( debug ) log.debug('ms getComponentForEntity ' + componentDef.id + ' ' + entity.id + ' ' + query.getSql() + ' ' + JSON.stringify(query.getParameters()) );

        return self.allRows( query.getSql(), query.getParameters(), function(err, rows){
            if( err ){
                log.error('error getComponentForEntity ' + err );
                return callback(err);
            }
            if( !rows || rows.length <= 0 ){
                return callback( componentDef.schema.id + ' not found for entity ' + entity.id, null, entity );
            }

            if( debug ) log.debug('returned ' + rows.length + ' rows');
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
    removeComponent: function( componentDef, entity, options, callback ){
        var self = this;

        var processEntity = function(err,entity){
            if( err ){ log.error('error retrieving entity ' + err ); return callback(err); }
            if( options.debug ){ print_ins( arguments ); log.debug('removeComponent: ' + JSON.stringify(entity) );}

            var bf = entity.get('component_bf');

            // remove the reference from the entities bitfield
            log.debug('unsetting ' + componentDef.id );
            entity.get('component_bf').set( componentDef.id, false );

            // save the update
            return self.saveEntity( entity, null, function(err,entity){

                // delete the component
                var query = Schema.toDelete( componentDef.schema.id, {where:{entity_id:entity.id}} );

                // print_ins( query,2 );
                return self.execQuery( query, {values:[entity.id], debug:true}, function(err, lastId){
                    // print_ins( arguments,2 );
                    if( err ){ log.error('error deleting component: ' + err); print_ins(err.errno); return callback(err); }
                    return callback(null);
                });

                // return callback(null);
            });
        };

        if( options.updateEntity === false ){
            // log.debug('not updating entity ' + entity.id );
            // delete the component
            var query = Schema.toDelete( componentDef.schema.id, {where:{entity_id:entity.id}} );
            return self.execQuery( query, {values:[entity.id], debug:true}, function(err, lastId){
                return callback(err);
            });
        }

        // retrieve the entity before processing
        return this.getEntity( entity, options, processEntity );
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
            

            // insert the def in the component_def table
            var query = Schema.toInsert( '/schema/component_def' );

            query.set('uri', componentDef.schema.id );
            query.set('schema', componentDef.schema );// JSON.stringify(componentDef.schema) );

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
            log.info('retrieved ' + rows.length + ' existing component defs');

            var componentSchemas = rows.map( function(row){
                return {id:row.id, schema:JSON.parse(row.schema) };
            });

            var componentDefs = self.registry.registerComponent( componentSchemas, {persist:false} );

            return callback(null, componentDefs);
        });
        // });
    },


    /**
     * Creates a new entityset
     * 
     * @param  {[type]}   options  [description]
     * @param  {Function} callback [description]
     * @return {[type]}            [description]
     */
    createEntitySet: function( attrs, options, callback ){
        var self = this;
        // print_ins( EntitySet );
        options = _.extend( {Model:EntitySet.Model}, options );
        var result = EntitySet.create( attrs, this, this.registry, options );

        if( !options.reload ){
            self.trigger('entity_set:create', result );
            return callback(null, result);
        }
        return result.reload( options, function(err,pEntitySet){
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
        if( options.debug ){
            // log.debug( query.getSql() + ' ' + JSON.stringify(values) );
            // log.debug( JSON.stringify(values) );
        }
        query = _.isString(query) ? query : query.getSql();

        return this.dbHandle.run( query, values, function(err){
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


    count: function( statement, options, callback ){
        var sql = statement.getSql();

        // TODO : replace this with something more elegant
        sql = sql.replace( 'SELECT ', 'SELECT COUNT(*) AS count, ');

        return this.dbHandle.get( sql, function(err,row){
            if( err ){ log.error( err); return callback(err); }

            return callback(null, row.count);
        });
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
            if( err ){ log.error( '_open error for ' + this.filename + ':' + err ); return callback(err); }
            log.debug('opened db ' + self.filename );
            // self.dbHandle.on('profile', function(sql,time){
            //     log.debug('profile: (' + time + ') ' + sql);
            // });
            // TODO : control these options from config
            return self.dbHandle.serialize(function(){
                self.dbHandle.run("PRAGMA synchronous = OFF");
                self.dbHandle.run("PRAGMA journal_mode = MEMORY");
                
                return callback(null, self.dbHandle);
            });
        });
    },

    _close: function(callback){
        this.dbHandle.close();
        this.dbHandle = null;
        return null;
    }

});