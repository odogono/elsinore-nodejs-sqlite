require('./common');

var sqlite3 = require('sqlite3').verbose();

var Storage = require('../');
var Schema = require('../lib/schema');

// var initRegistryWithSql = function( sqlitePath, sqlFixturePath, callback ){
//     var registry, storage;
//     Storage.loadSql( sqlitePath, fs.readFileSync( Common.pathFixture(sqlFixturePath)).toString(), function(err, storage){
//         odgnEntity.Registry.create({initialize:true, storage:storage, filename:sqlitePath}, function(err, registry){
//             if( err ) throw err;
//             return callback( err, registry, registry.storage );
//         });
//     });
// }


describe('EntitySet', function(){

    describe('Init', function(){

        it('should create an entity set', function(done){
            initRegistryWithSql(':memory:', 'content_sets.sql', function(err, registry, storage){

                var esAttrs = {
                    select:[    '/component/poi', 
                                '/component/status', 
                                {schemaId:'/component/content_set/member'},
                                {schemaId:'/component/content_set', 
                                    on:'content_set_member.content_set_id=content_set.entity_id', 
                                    where:{code:'CSB'} }]
                };

                registry.createEntitySet( esAttrs, {reload:true}, function(err, entitySet){

                    assert.equal( entitySet.get('entity_count'), 4 );
                    // entitySet.forEach( function(entity, es){
                    //     print_ins( es.getComponent('/component/poi', entity).attributes );
                    // });

                    assert.equal( entitySet.getComponent( "/component/poi", 106 ).get('title'), 'poi 005');
                    done();
                });

            });
        });

        it.skip('should initialise from existing', function(done){
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
        
    });

});