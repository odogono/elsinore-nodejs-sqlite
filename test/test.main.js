require('./common');

var sqlite3 = require('sqlite3').verbose();


describe('odgn-entity-sqlite', function(){
    before( function(done){
        this.db = new sqlite3.Database(':memory:');
        done();
    });

    describe('main', function(){
        
    });

    describe('Schema', function(){

        it('should convert a schema to sql', function(){

            var SqliteSync = require('../')(odgn);

            SqliteSync.Schema.register({
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

            assert.equal( SqliteSync.Schema.toSql('/schema/entity'),
                'CREATE TABLE IF NOT EXISTS tbl_entity( id INTEGER PRIMARY KEY, _status INTEGER, _created_at DATETIME DEFAULT CURRENT_TIMESTAMP, _created_by INTEGER, _updated_at DATETIME DEFAULT CURRENT_TIMESTAMP, _updated_by INTEGER);');
        });
    });

    describe('Entity', function(){

        it('should create a new entity with an id', function(done){
            var self = this;

            var SqliteSync = require('../')(odgn,{filename:'entity.sqlite'});
            odgn.entity.EntityRegistry.use( SqliteSync );

            var eRegistry = odgn.entity.EntityRegistry.create();
            var entityId;

            async.waterfall([
                function(cb){
                    eRegistry.initialise(cb);                    
                },
                function(cb){
                    eRegistry.create(cb);
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

});