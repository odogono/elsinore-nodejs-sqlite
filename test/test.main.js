require('./common');

var sqlite3 = require('sqlite3').verbose();

var Storage = require('../');
var Schema = require('../lib/schema');

describe('odgn-entity-sqlite', function(){
    beforeEach( function(done){
        var self = this;
        this.registry = odgnEntity.Registry.create({initialize:true, storage:Storage, filename:'ecs.sqlite', clearAll:true}, function(err,registry){
            self.registry = registry;
            self.storage = registry.storage;
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


    describe.skip('Entity', function(){
        it('should create a new entity with an id', function(done){
            var self = this;
            self.registry.createEntity(function(err,entity){
                // print_ins( entity.toJSON() );
                assert( entity.id );
                done();
            });
        });
    });

    describe.skip('Entity Import', function(){
        it.skip('should load an entity and its components from data', function(done){
            var self = this;
            var data = Common.readFixture('entity.json',true);
            // self.registry.on('all', function(evt){
            //     log.debug('registry evt ' + evt);
            // });
            self.registry.importEntity( data, null, function(err, entity){
                return entity.getComponent('/component/human_name', function(err,component,entity){
                    assert.equal( component.get('first_name'), 'dummy' );
                    assert.equal( component.get('last_name'), 'testman');
                    assert.equal( component.get('age'), 51 );
                    done();
                });
            });
        });
    });

    describe.skip('Entity Components', function(){
        it('should add a component to an entity', function(done){
            var self = this, entity;
            async.waterfall([
                function(cb){
                    self.registry.createEntity(cb);
                },
                function(pEntity,cb){
                    entity = pEntity;
                    entity.addComponent("/component/email", cb);
                },
                function(pComponent,pEntity,cb){
                    assert( odgnEntity.Component.isComponent(pComponent) );
                    // note - getting a component direct from the entity is
                    // not a great way to do it. better from an entityset
                    pEntity.getComponent('/component/email', cb);
                }
            ], function(err, pComponent,pEntity){
                assert( pComponent );
                done();
            });
        });

        it('should add a serialised component to an entity', function(done){
            var self = this, entity;
            async.waterfall([
                function(cb){
                    self.registry.createEntity(cb);
                },
                function(pEntity,cb){
                    entity = pEntity;
                    entity.addComponent({schemaId:"/component/email", email:"alex@odogono.com", "is_allowed":true}, cb);
                },
                function(pComponent,pEntity,cb){
                    assert( odgnEntity.Component.isComponent(pComponent) );
                    // note - getting a component direct from the entity is
                    // not a great way to do it. better from an entityset
                    pEntity.getComponent('/component/email', cb);
                }
            ], function(err, pComponent,pEntity){
                assert.equal( pComponent.get("email"), "alex@odogono.com" );
                assert.equal( pComponent.get("is_allowed"), true );
                done();
            });
        });
    });


    describe('Component', function(){

        it.skip('should instantiate a component from data', function(done){
            var self = this;
            var componentData = {
                id: 19,
                entity_id: 6,
                tags:[ 'super', 'cali', 'frag' ]
            };

            var component = self.registry.createComponent('/component/tags', componentData, {instantiate:true, debug:true} );
            assert.equal( component.id, 19 );
            assert.equal( component.get('tags')[1], 'cali');
            assert.equal( component.entityId, 6 );
            
            done();
        });

        it('should update an existing component', function(done){
            var self = this;
            self.registry.createComponent('/component/tags', {entity_id:16, tags:[ 'alpha', 'beta', 'gamma' ]}, {debug:true}, function(err, component){

                self.storage.retrieveComponentById('/component/tags', component.id, null, function(err, component){

                    component.set( 'tags', ['what', 'the', 'fox', 'say'] );

                    self.registry.updateComponent( component, 16, null, function(err,component){
                        done();
                    });

                    // print_ins( component,1 );
                //     assert.equal( component.get('first_name'), 'alex' );
                //     assert.equal( component.get('last_name'), 'veenendaal' );

                    
                });
            });
        });

        it.skip('should create a component with supplied attributes', function(done){
            var self = this;
            self.registry.createComponent('/component/human_name', {first_name:'alex', last_name:'veenendaal'}, null, function(err, component){
                
                self.storage.retrieveComponent('/component/human_name', {where:"first_name='alex'"}, function(err, component){

                    assert.equal( component.get('first_name'), 'alex' );
                    assert.equal( component.get('last_name'), 'veenendaal' );

                    done();    
                });
            });
        });

        it.skip('should create a component with supplied attributes', function(done){
            var self = this;
            // log.debug('---')
            self.registry.createComponent( '/component/human_name', 
                [{first_name:'ian', last_name:'palmer', age:45},{first_name:'paul', last_name:'barrett', age:38}]
                ,null, function(err,component){
                
                self.storage.retrieveComponent('/component/human_name', {where:"last_name='barrett'"}, function(err, component){

                    assert.equal( component.get('first_name'), 'paul' );
                    assert.equal( component.get('last_name'), 'barrett' );
                    assert.equal( component.get('age'), 38);

                    done();    
                });

            });
        })
    });

    describe.skip('EntitySet', function(){
        it.skip('should populate with existing components', function(done){
            var self = this;
            var entityId;
            async.waterfall([
                function(cb){
                    self.registry.createEntity(cb);
                },
                function(entity,cb){
                    entity.addComponent('/component/email', cb);
                },
                function(pEntity,pComponent,cb){
                    entityId = pEntity.id;
                    // create an entityset interested in a single component
                    self.registry.createEntitySet( {componentDefs:'/component/email'}, cb );
                }
            ], function(err,pEntitySet){
                if( err ) throw err;
                assert( pEntitySet.hasEntity( entityId ) );
                done(); 
            });
        });
    });

    describe.skip('main', function(){
        
        it('should have registered components', function(done){
            var self = this, registry = self.registry, eid;
            registry.createEntity( function(err, entity){
                registry.hasEntity( entity.id, function(err,entityId){
                    assert.equal( entity.id, entityId );
                    done();
                });
            });
        });
        
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
                    pEntity.getComponent('/component/human_name', cb);
                }
            ], function(err, pComponent,pEntity){
                assert( pComponent );
                done();
            });
        });

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