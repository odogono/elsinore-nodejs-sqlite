var sqlite3 = require('sqlite3').verbose();



module.exports = function(odgnEntity, options){
    var options = options || {};
    var Entity = odgnEntity.entity || odgnEntity;
    var Schema = require('./schema')(Entity);

    var dbFilename = options.filename || ':memory:';

    var EntityStorage = function(){
        
    };


    var openDb = function(callback){
        var self = this;
        return self.db = new sqlite3.cached.Database( dbFilename, function(err){
            if( err ){
                return callback(err);
            }
            return callback(null,self.db);
        });
    }

    var closeDb = function(){
        return self.db.close();
    }

    // log.debug('using ' + Entity.EntityRegistry );
    // print_ins( Entity.EntityRegistry );

    EntityStorage.prototype.initialise = function(registry,options,callback){
        var self = this, db = this.db;
        log.debug('initialising sqlite entity registry');

        openDb(function(err,db){
            if( err ){
                return callback(err);
            }

            // queue an operation for creating tables from the
            // default schemas
            var q = async.queue(function(schema,cb){
                Schema.register(schema);
                var sql = Schema.toSql( schema.id );
                db.run(sql,cb);
            },1);

            _.each( Schema.schemas, q.push );

            // called when finished
            q.drain = function(){
                callback(null,registry);
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

        openDb(function(err,db){
            if( err ){
                return callback(err);
            }
            db.run('INSERT INTO tbl_entity (_status) VALUES (?);', [0], function(err){
                if( err ){
                    return callback(err);
                }
                var entity = registry._create();
                entity.id = this.lastID;
                return callback(null,entity);
            });
        });
    };

    EntityStorage.prototype.read = function( entityId, registry, options, callback ){
        var self = this;

        openDb(function(err,db){
            if( err ){
                return callback(err);
            }
            return self.db.get('SELECT * FROM tbl_entity WHERE id = ? LIMIT 1', [entityId], function(err,row){
                if( err ){
                    return callback(err);
                }
                var entity = registry._parse( row );
                callback( null, entity );
            });
        });
    };

    var removeAllTables = function(options, callback){
        if( arguments.length == 1 ){
            callback = options;
            options = {};
        }
        var self = this;
        log.debug('removing all tables');
        openDb(function(err,db){
            if( err ){
                return callback(err);
            }
            async.waterfall([
                function(cb){
                    db.all("SELECT name FROM sqlite_master WHERE type = 'table'", cb);
                },
                function(rows,cb){
                    async.eachSeries( rows, function(row,rowCb){
                        log.debug('removing table ' + row.name);
                        db.get("DROP TABLE " + row.name,rowCb);
                    }, cb );
                }
            ], callback );
        });
    };

    var result = {
        Schema: require('./schema')(Entity),
        EntityRegistry: EntityStorage,
        util:{
            clearAll: removeAllTables
        }
        // EntityRegistry:{
        //     initialise: entityRegistryInitialise,
        //     create: entityRegistryCreate
        // }
    };
    // _.bindAll( result.EntityRegistry );

    return result;
};