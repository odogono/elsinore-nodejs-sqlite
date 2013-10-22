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

        it.skip('should', function(done){
            initRegistryWithComponentsAndImport(
                'test.sqlite', 'components.set_a.json', 'entity.import.a.json', 
                function(err,registry,storage){
                    log.debug('ok');
                    done();
                });
        });

        it('should initialise from existing', function(done){
            var self = this;

            initRegistryWithSql(':memory:', 'basic.sql', function(err, registry, storage){
            // Storage.loadSql( ':memory:', fs.readFileSync( Common.pathFixture('basic.sql')).toString(), function(err, storage){
                // self.registry = odgnEntity.Registry.create({initialize:false, storage:storage}, function(err, registry){
                    // self.storage = registry.storage;

                    // print_ins( storage );
                    // assert( !storage.isNew );

                    storage.retrieveComponent('/component/human_name', {where:"last_name='fixture'"}, function(err, component){

                        assert.equal( component.get('first_name'), 'charlie' );
                        assert.equal( component.get('last_name'), 'fixture' );
                        assert.equal( component.get('location'), 'flatland' );

                        done();    
                    });
                // });
            });
        });

        it.skip('should initialise from new ', function(done){
            var self = this;
            self.registry = odgnEntity.Registry.create({initialize:true, storage:Storage}, function(err, registry){
                self.storage = registry.storage;
                assert( self.storage.isNew );
                done();
            });
        });

        it.skip('should initialise from existing', function(done){
            var self = this;
            var sqlitePath = Common.pathVar('ecs.sqlite');
            Storage.loadSql( sqlitePath, fs.readFileSync( Common.pathFixture('basic.sql')).toString(), function(err, storage){
                // log.info( sqlitePath );

                self.registry = odgnEntity.Registry.create({initialize:true, storage:Storage, filename:sqlitePath}, function(err, registry){
                    self.storage = registry.storage;
                    assert( !self.storage.isNew );
                    
                    self.storage.retrieveComponent('/component/human_name', {where:"last_name='fixture'"}, function(err, component){
                        assert.equal( component.get('first_name'), 'charlie' );
                        sh.rm( sqlitePath );
                        done();
                    });

                });
            });
        });

    });

});