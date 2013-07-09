
module.exports = function(odgn,options){

    var dbOpen = options.dbOpen;
    var dbClose = options.dbClose;

    var ComponentStorage = function(){
    };


    var upsertComponentDef = function( def, options, callback ){
        // var sql = odgn.entity.Sqlite.Schema.toInsert('/schema/component_def');
        var sql = 'INSERT INTO tbl_component_def (uri,schema) VALUES (?,?)';
        dbOpen(function(err,db){
            if( err ){ return callback(err); }

            var insertValues = [def.schema.id, JSON.stringify(def.schema) ];

            db.run( sql, insertValues, function(err){
                callback(err);    
            });
        });
    };

    var retrieveComponentDef = function( uri, callback ){

    };

    var deleteComponentDef = function( uri, callback ){

    };

    ComponentStorage.prototype.initialise = function(registry,options,callback){
        var self = this;//, db = this.db;
        log.debug('initialising sqlite component registry');

        dbOpen(function(err,db){
            if( err ){ return callback(err); }

            if( options.clearAll ){
                log.debug('clearing component registry' );
                db.exec('DELETE FROM tbl_component_def; DELETE FROM tbl_component;', function(err){
                    registry.trigger('initialise', registry, options );
                    callback( null, registry );
                });
                
            } else {
                registry.trigger('initialise', registry, options );
                callback( null, registry );    
            }
        });
    };


    ComponentStorage.prototype.registerComponent = function( def, registry, options, callback){
        var self = this;

        log.debug('registering component ' + def.schema.id );
        registry.trigger('register', def, registry, options );

        // insert into "/schema/component_def"
        upsertComponentDef( def, options, function(err){
            if( err ){ return callback(err); }

            // create the component table
            callback();
        });

        // return async.nextTick(function(){
        //     return callback(null, def);
        // });
    };

    ComponentStorage.prototype.createComponent = function( def, attrs, registry, options, callback ){
        var self = this;
        log.debug('creating component ' + def.schema.id );

        dbOpen(function(err,db){
            if( err ){ return callback(err); }

            var sql = odgn.entity.Sqlite.Schema.toInsert( def.schema.id );
            log.debug( sql );
        });

        // return async.nextTick(function(){
        //     def.create( attrs, options, function(err,com){
        //         registry.trigger('create', com, def, self, options);
        //         return callback(null, com);
        //     });
        // });
    }

    return {
        ComponentStorage: ComponentStorage
    }
};