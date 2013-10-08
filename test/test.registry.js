require('./common');

var sqlite3 = require('sqlite3').verbose();

var Storage = require('../');
var Schema = require('../lib/schema');

describe('odgn-entity-sqlite', function(){
    /*beforeEach( function(done){
        var self = this;

        this.registry = odgnEntity.Registry.create({initialize:true, storage:Storage, filename:'ecs.sqlite', clearAll:true}, function(err,registry){
            self.registry = registry;
            self.storage = registry.storage;

            // var components = JSON.parse( fs.readFileSync( Common.pathFixture('components.json') ) );
            // self.registry.registerComponent( components, null, function(){
                done();
            // });    
        });
    });//*/

    describe('Entity', function(){
        it('should initialise from an existing repo', function(done){
            var self = this;

            Storage.loadSql( ':memory:', fs.readFileSync( Common.pathFixture('basic.sql')).toString(), function(err, storage){

                self.registry = odgnEntity.Registry.create({initialize:true, storage:storage}, function(err, registry){
                    self.storage = registry.storage;

                    self.storage.retrieveComponent('/component/name', {where:"last_name='fixture'"}, function(err, component){

                        assert.equal( component.get('first_name'), 'charlie' );
                        assert.equal( component.get('last_name'), 'fixture' );
                        assert.equal( component.get('location'), 'flatland' );

                        done();    
                    });

                });

            });
        });
        
    });

});