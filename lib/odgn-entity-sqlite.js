var sqlite3 = require('sqlite3').verbose();



module.exports = function(odgnEntity, options){
    var options = options || {};
    var Entity = odgnEntity.entity || odgnEntity;
    var Schema = require('./schema')(Entity);

    var dbFilename = options.filename || ':memory:';

    var EntityStorage = function(){
        
    };

    // log.debug('using ' + Entity.EntityRegistry );
    // print_ins( Entity.EntityRegistry );

    EntityStorage.prototype.initialise = function(registry,options,callback){
        var self = this, db = this.db;
        log.debug('initialising sqlite entity registry');

        this.db = new sqlite3.Database(dbFilename, function(err){
            if( err ){
                return callback(err);
            }

            // queue an operation for registering the schemas
            var q = async.queue(function(schema,cb){
                Schema.register(schema);
                var sql = Schema.toSql( schema.id );
                self.db.run(sql,cb);
            },1);

            _.each( Schema.schemas, q.push );

            q.drain = function(){
                callback();
            };
        });
    };

    /**
     * Creating a new entity
     * @param  {[type]}   options  [description]
     * @param  {Function} callback [description]
     * @return {[type]}            [description]
     */
    EntityStorage.prototype.create = function( registry, options, callback){
        var self = this;

        self.db.run('INSERT INTO tbl_entity (_status) VALUES (?);', [0], function(err){
            if( err ){
                return callback(err);
            }
            var entity = registry._create();
            entity.id = this.lastID;
            callback(null,entity);
        });
    };

    EntityStorage.prototype.read = function( entityId, registry, options, callback ){
        var self = this;

        return self.db.get('SELECT * FROM tbl_entity WHERE id = ? LIMIT 1', [entityId], function(err,row){
            if( err ){
                return callback(err);
            }
            var entity = registry._parse( row );
            callback( null, entity );
        });
    };

    var result = {
        Schema: require('./schema')(Entity),
        EntityRegistry: EntityStorage
        // EntityRegistry:{
        //     initialise: entityRegistryInitialise,
        //     create: entityRegistryCreate
        // }
    };
    // _.bindAll( result.EntityRegistry );

    return result;
};