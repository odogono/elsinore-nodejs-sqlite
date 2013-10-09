require('./common');

var sqlite3 = require('sqlite3').verbose();

var Storage = require('../');
var Schema = require('../lib/schema');

var initRegistryWithSql = function( sqlitePath, sqlFixturePath, callback ){
    var registry, storage;
    Storage.loadSql( sqlitePath, fs.readFileSync( Common.pathFixture(sqlFixturePath)).toString(), function(err, storage){
        odgnEntity.Registry.create({initialize:true, storage:storage, filename:sqlitePath}, function(err, registry){
            if( err ) throw err;
            return callback( err, registry, registry.storage );
        });
    });
}


describe('EntitySet', function(){

    describe('Init', function(){
        it('should initialise from existing', function(done){
            var self = this;
            var sqlitePath = 'entity_set.sqlite';

            odgnEntity.Registry.create({initialize:true, storage:Storage, filename:sqlitePath, clearAll:true}, function(err, registry){
                self.storage = registry.storage;
                assert( self.storage.isNew );
                
                var components = Common.readFixture('content_set.components.json', true );
                registry.registerComponent( components, null, function(){

                    // var data = Common.readFixture('entity_ids.json',true);
                    var data = Common.readFixture('content_sets.json',true);
                    registry.importEntity( data, null, function(err, entity){

                        done();
                    });
                });

            });
        });

        it.skip('should', function(done){
            initRegistryWithSql( ':memory:', 'content_sets.sql', function(err, registry, storage){

            });
        });
            // Storage.loadSql( sqlitePath, fs.readFileSync( Common.pathFixture('basic.sql')).toString(), function(err, storage){
            //     // log.info( sqlitePath );

            //     self.registry = odgnEntity.Registry.create({initialize:true, storage:Storage, filename:sqlitePath}, function(err, registry){
            //         self.storage = registry.storage;
            //         assert( !self.storage.isNew );
                    
            //         self.storage.retrieveComponent('/component/name', {where:"last_name='fixture'"}, function(err, component){
            //             assert.equal( component.get('first_name'), 'charlie' );
            //             sh.rm( sqlitePath );
            //             done();
            //         });

            //     });
            // });
    });

});