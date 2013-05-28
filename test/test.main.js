require('./common');

var sqlite3 = require('sqlite3').verbose();

var sync;

describe('odgn-entity-sqlite', function(){
    before( function(done){
        this.sync = sync = require('../')(odgn,{filename:'entity.sqlite'});
        odgn.entity.EntityRegistry.use( this.sync );
        done();
    });

    var createEntityRegistry = function(options, callback){
        if( arguments.length == 1 ){
            callback = options;
            options = {};
        }
        var eRegistry = odgn.entity.EntityRegistry.create();

        var init = function(){
            eRegistry.initialise( function(err, registry){
                callback(err,registry);
            });    
        }

        if( options.clearAll )
            return sync.util.clearAll(init);

        return init();
    };

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

            assert.equal( this.sync.Schema.toSql('/schema/entity'),
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

            log.debug( this.sync.Schema.toSql(schemaId) );

        });
    });

    
    

    describe('Entity', function(){

        it('should create a new entity with an id', function(done){
            var self = this;

            var eRegistry = odgn.entity.EntityRegistry.create();
            var entityId;

            async.waterfall([
                function(cb){
                    eRegistry.initialise(cb);                    
                },
                function(cb){
                    eRegistry.createEntity(cb);
                },
                function(entity,cb){
                    log.debug('created entity ' + entity.id );
                    entityId = entity.id;
                    assert( entity.id );    
                    eRegistry.read( entity.id, cb );
                },
            ], function(err,entity){
                assert.equal( entity.id, entityId );
                done(); 
            });
        });
    });

    describe('Component', function(){
        it('should register a component', function(done){
            var self = this;

            createEntityRegistry({clearAll:true}, function(err, registry){
                done()
            });
        });
    });

});