require('./common');

var sqlite3 = require('sqlite3').verbose();

var Storage = require('../');
var Schema = require('../lib/schema');
var SqlStatement = Schema.SqlStatement;

describe('odgn-entity-sqlite', function(){
    beforeEach( function(done){
        // var self = this;
        // this.registry = odgnEntity.Registry.create({initialize:true}, function(err,registry){
        //     self.registry = registry;
        done();
        // });
    });

    describe('main', function(){
        
    });

    describe('SqlStatement', function(){
        // it('should assign the sql attribute', function(){
        //     var stat = new SqlStatement();
        //     stat.set( 'sql', 'SELECT * from tbl_test' );
        //     print_ins( stat );
        //     assert.equal( stat.get('sql'), 'SELECT * from tbl_test' );
        // })
    });


    describe('EntitySet', function(){

        it('should', function(done){

            initRegistryWithSql(':memory:', 'content_sets.sql', function(err, registry, storage){

                // log.debug( odgnEntity.Schema.titleFromSchema('/component/content_set/member') );
                // process.exit();

                var statement = Schema.toComponentSelect( 
                    [ '/component/poi', '/component/status', 
                    {schemaId:'/component/content_set/member'},
                    {schemaId:'/component/content_set', on:'content_set_member.content_set_id=content_set.entity_id', where:{code:'CSB'} }
                ] );
                
                print_ins( statement.getSql() );
                // print_ins( odgnEntity.Schema.getSchema('/component/content_set') );

                storage.allRows( statement.getSql(), null, function(err,rows){
                    print_ins( rows );
                    done();    
                });
            });

        });
        

    });

    describe.skip('Schema', function(){

        it('should create a basic select statement from a schema', function(){
            var stub = statementStub( 'tbl_test', 'SELECT * FROM tbl_test' );
            var statement = Schema.toSelect('/component/test');
            stub.restore();
        });

        it('should obey the limit parameter', function(){
            var stub = statementStub( 'tbl_test', 'SELECT * FROM tbl_test LIMIT 1' );
            var statement = Schema.toSelect('/component/test', {limit:1} );
            stub.restore();
        });

        it('should create a basic select with where statement from a schema', function(){
            var stub = statementStub( 'tbl_test', 'SELECT * FROM tbl_test WHERE id=?', null, [45] );

            var statement = Schema.toSelect('/component/test', {where:{id:45}} );
            stub.restore();
        });


        it('should create a select with specified values', function(){
            var stub = statementStub( 'tbl_test', 'SELECT alpha,beta FROM tbl_test' );
            var statement = Schema.toSelect('/component/test', {columns:['alpha','beta']} );
            stub.restore();
        }); 

        /*
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

        });//*/
    });

    
    

    // describe('Entity', function(){

    //     it('should create a new entity with an id', function(done){
    //         var self = this;
    //         var entityId, eRegistry;

    //         async.waterfall([
    //             function(cb){
    //                 odgn.entity.EntityRegistry.create({clearAll:true},cb);
    //             },
    //             function(registry,cb){
    //                 (eRegistry = registry).createEntity(cb);
    //             },
    //             function(entity,cb){
    //                 entityId = entity.id;
    //                 assert( entity.id );    
    //                 eRegistry.getEntity( entity.id, cb );
    //             },
    //         ], function(err,entity){
    //             if( err ){ return log.error(err); }
    //             assert.equal( entity.id, entityId );
    //             done(); 
    //         });
    //     });
    // });

    // describe('Component', function(){
    //     it.only('should register a component', function(done){
    //         var self = this;
    //         var cRegistry;
    //         var componentDef = {
    //             "id":"/component/data",
    //             "type":"object",
    //             "properties":{
    //                 "name":{ "type":"string" },
    //                 "count":{ "type":"integer" }
    //             }
    //         };

    //         async.waterfall([
    //             function(cb){
    //                 // create a new registry instance
    //                 log.debug('1 initialising registry');
    //                 self.registry.initialise({clearAll:true}, cb);
    //             },
    //             function(registry,cb){
    //                 log.debug('2 registering component');
    //                 (cRegistry = registry).registerComponent(componentDef,cb);
    //             },
    //             function( componentDef,cb ){
    //                 log.debug('3 retrieving def');
    //                 cRegistry.getComponentDef('/component/data', cb );
    //             }
    //         ], function(err,def){
    //             if( err ) log.error( err );
    //             assert.equal( def.id, '/component/data' );
    //             done();
    //         });
    //     });
    // });

    // describe('Component', function(){
    //     it('should create a component from a def', function(done){
    //         var self = this;
    //         var cRegistry;
    //         var componentDef = {
    //             "id":"/component/data",
    //             "type":"object",
    //             "properties":{
    //                 "name":{ "type":"string" },
    //                 "count":{ "type":"integer" }
    //             }
    //         };

    //         async.waterfall([
    //             function(cb){
    //                 // create a new registry instance
    //                 self.registry.initialise({clearAll:true}, cb);
    //             },
    //             function(registry,cb){
    //                 (cRegistry = registry).registerComponent(componentDef,cb);
    //             },
    //             function( registry,cb ){
    //                 cRegistry.createComponent('/component/data', {'name':'diamond', 'count':23}, cb );
    //             }
    //         ], function(err,component){
    //             if( err ) log.error( err );
    //             assert.equal( component.get("name"), "diamond" );
    //             assert.equal( component.get("count"), 23 );
    //             done();
    //         });
    //     });
    // });
});


var statementStub = function( tableName, sql, columns, parameters ){
        return sinon.stub( Schema, '_statementForSchema', function(){
            return {
                get: function(){
                    return tableName;
                },
                set: function(key,val){
                    if( sql && key == 'sql' )
                        assert.equal( val, sql );
                    if( columns && key == 'columns' )
                        assert.deepEqual(val, columns );
                    if( parameters && key == 'parameters' )
                        assert.deepEqual(val, parameters );
                }
            }
        })
    };