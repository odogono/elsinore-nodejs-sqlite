require('./common');

var sqlite3 = require('sqlite3').verbose();

var Storage = require('../');

describe('odgn-entity-sqlite', function(){
    beforeEach( function(done){
        var self = this;
        this.registry = odgnEntity.Registry.create({initialize:true, storage:Storage, filename:'ecs.sqlite', clearAll:true}, function(err,registry){
            self.registry = registry;
            // registry.useStorage( Storage, {filename:'ecs.sqlite', clearAll:true}, function(err, storage){
                // self.registry.on('component:register', function(componentDef){
                //     log.debug('registry registered component: ' + componentDef.schema.id + '(' + componentDef.id + ')');
                // });
                var components = JSON.parse( fs.readFileSync( Common.pathFixture('components.json') ) );
                self.registry.registerComponent( components, null, function(){
                    done();
                });    
            // });
        });
    });

    describe('main', function(){
        
        // it('should have registered components', function(done){
        //     var self = this, registry = self.registry, eid;
        //     registry.createEntity( function(err, entity){
        //         registry.hasEntity( entity.id, function(err,entityId){
        //             assert.equal( entity.id, entityId );
        //             done();
        //         });
        //     });
        // });
        /*
        it('should add a component to an entity', function(done){
            var self = this, entity;
            async.waterfall([
                function(cb){
                    self.registry.createEntity(cb);
                },
                function(pEntity,cb){
                    entity = pEntity;
                    entity.addComponent("/component/name", cb);
                },
                function(pComponent,pEntity,cb){
                    assert( odgnEntity.Component.isComponent(pComponent) );
                    // note - getting a component direct from the entity is
                    // not a great way to do it. better from an entityset
                    pEntity.getComponent('/component/name', cb);
                }
            ], function(err, pComponent,pEntity){
                assert( pComponent );
                done();
            });
        });//*/

        it('create an entity from a template', function(done){
            var self = this;
            var entityTemplate = {
                "id":"/entity/template/example",
                "type":"object",
                "properties":{
                    "a":{ "$ref":"/component/tmpl/a" },
                    "c":{ "$ref":"/component/tmpl/c" },
                }
            };
            var entity;

            async.waterfall([
                function(cb){
                    self.registry.registerComponent([ "/component/tmpl/a", "/component/tmpl/b", "/component/tmpl/c" ], null, cb);
                },
                function(components, cb){
                    self.registry.registerEntityTemplate( entityTemplate, null, cb);
                },
                function( defs, cb ){
                    self.registry.createEntityFromTemplate( '/entity/template/example', cb );
                },
                function(result, cb){
                    entity = result;
                    self.registry.getEntitiesWithComponents('/component/tmpl/c', cb);
                },
                function( pEntities,pComponentDefs, cb){
                    assert.equal( pEntities.length, 1 );
                    assert.equal( pEntities[0].id, entity.id );
                    // retrieve all the components for this entity
                    self.registry.getEntityComponents( entity, {}, cb );
                },
            ], function(err, components){
                assert.equal( components[0].schemaId, '/component/tmpl/a' );
                assert.equal( components[1].schemaId, '/component/tmpl/c' );
                done();  
            });
        });

    });
});