require('./common');

var sqlite3 = require('sqlite3').verbose();

var storage;

describe('odgn-entity-sqlite', function(){
    before( function(done){
        this.registry = odgn.entity.Registry.create();
        this.storage = storage = require('../');//(odgn,{filename:'entity.sqlite'});

        this.registry.use( this.storage, {filename:'entity.sqlite'} );

        done();
    });


    describe('main', function(){
        
    });

    describe('Schema', function(){

        it('should convert a schema to sql', function(){

            this.sync.Schema.register({
                "id":"/schema/entity",
                "title":"entity",
                "type":"object",
                "properties":{
                    "id":{ "type":"integer" },
                    "_status":{ "type":"integer" },
                    "_created_at":{ "type":"string", "format":"date-time" },
                    "_created_by":{ "type":"integer" },
                    "_updated_at":{ "type":"string", "format":"date-time" },
                    "_updated_by":{ "type":"integer" },
                },
                "primaryKey":"id"
            });

            assert.equal( this.sync.Schema.toCreate('/schema/entity'),
                'CREATE TABLE IF NOT EXISTS tbl_entity( id INTEGER PRIMARY KEY, _status INTEGER, _created_at DATETIME DEFAULT CURRENT_TIMESTAMP, _created_by INTEGER, _updated_at DATETIME DEFAULT CURRENT_TIMESTAMP, _updated_by INTEGER);');
        });

        it('should convert another', function(){
            var schemaId = this.sync.Schema.register({
                "id":"/component/data",
                "type":"object",
                "properties":{
                    "name":{ "type":"string" },
                    "count":{ "type":"integer" }
                }
            });

            log.debug( this.sync.Schema.toCreate(schemaId) );

        });
    });

    
    

    describe('Entity', function(){

        it('should create a new entity with an id', function(done){
            var self = this;
            var entityId, eRegistry;

            async.waterfall([
                function(cb){
                    odgn.entity.EntityRegistry.create({clearAll:true},cb);
                },
                function(registry,cb){
                    (eRegistry = registry).createEntity(cb);
                },
                function(entity,cb){
                    entityId = entity.id;
                    assert( entity.id );    
                    eRegistry.readEntity( entity.id, cb );
                },
            ], function(err,entity){
                if( err ){ return log.error(err); }
                assert.equal( entity.id, entityId );
                done(); 
            });
        });
    });

    describe('Component', function(){
        it.only('should register a component', function(done){
            var self = this;
            var cRegistry;
            var componentDef = {
                "id":"/component/data",
                "type":"object",
                "properties":{
                    "name":{ "type":"string" },
                    "count":{ "type":"integer" }
                }
            };

            async.waterfall([
                function(cb){
                    // create a new registry instance
                    log.debug('1 initialising registry');
                    self.registry.initialise({clearAll:true}, cb);
                },
                function(registry,cb){
                    log.debug('2 registering component');
                    (cRegistry = registry).registerComponent(componentDef,cb);
                },
                function( registry,cb ){
                    registry.getComponentDef('/component/data', cb );
                }
            ], function(err,def){
                if( err ) log.error( err );
                print_ins( def );
                // assert.equal( component.get("name"), "diamond" );
                // assert.equal( component.get("count"), 23 );
                done();
            });
        });
    });

    describe('Component', function(){
        it('should create a component from a def', function(done){
            var self = this;
            var cRegistry;
            var componentDef = {
                "id":"/component/data",
                "type":"object",
                "properties":{
                    "name":{ "type":"string" },
                    "count":{ "type":"integer" }
                }
            };

            async.waterfall([
                function(cb){
                    // create a new registry instance
                    self.registry.initialise({clearAll:true}, cb);
                },
                function(registry,cb){
                    (cRegistry = registry).registerComponent(componentDef,cb);
                },
                function( registry,cb ){
                    cRegistry.createComponent('/component/data', {'name':'diamond', 'count':23}, cb );
                }
            ], function(err,component){
                if( err ) log.error( err );
                assert.equal( component.get("name"), "diamond" );
                assert.equal( component.get("count"), 23 );
                done();
            });
        });
    });
});