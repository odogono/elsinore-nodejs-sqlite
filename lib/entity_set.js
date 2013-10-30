var Schema = require('./schema');

var EntitySet = odgnEntity.EntitySet.Model.extend({

    initialize: function( attrs, options ){
        odgnEntity.EntitySet.Model.prototype.initialize.apply( this, arguments );

        // if( attrs.select ){
        //     log.debug('examining select ');
        // }

    },

    isComponentOfInterest: function( componentDef ){
        log.debug('not interested in ' + componentDef.id );
        return false;
    },

    /**
     * 
     * @param  {Function} callback [description]
     * @return {[type]}            [description]
     */
    reload: function(callback){
        var self = this;
        var statement = Schema.toComponentSelect( this.get('select') );
        var tables = statement.get('_tables');
        var sql = statement.getSql();
        var order = this.get('order');

        var limit = this.get('page_size') || 10;
        if( this.get('limit') === false )
            limit = null;

        log.debug('entity set limit is ' + limit );
        // log.debug('page is ' + this.get('page') );
        // log.debug('page_size is ' + this.get('page_size') );
        var offset = (this.get('page')-1) * this.get('page_size');

        // TODO : recalculate sql only if the entity_set has changed

        var createComponentOptions = { instantiate:true, debug:true };
        if( order ){
            sql = sql + ' ORDER BY ';
            for( var orderKey in order ){
                sql = sql + ' ' + orderKey + ' ' + order[orderKey];
            }    
        }
        if( limit )
            sql = sql + ' LIMIT ' + limit + ' OFFSET ' + offset;

        // log.debug( sql );
        // print_ins(sql); process.exit();
        
        // do a count on the returned rows first
        return this.storage.count( statement, null, function(err,count){

            if( err ){
                log.error('error reloading entity_set : ' + err );
            }

            var pageSize = self.get('page_size');
            // log.debug('offset is ' + offset );
            // log.debug('count is ' + count );
            // log.debug('page is ' + (Math.floor(offset/pageSize)) );
            self.set('page', (Math.floor(offset/pageSize)+1)); //offset Math.floor( count / offset ) + 1 );
            self.set('entity_count', count );
            self.set('start', offset);
            self.set('page_count', count == 0 ? 1 : Math.ceil(count / pageSize) );
            // log.debug('page_count is ' + self.get('page_count') );

            return self.storage.allRows( sql, null, function(err,rows){

                _.each( rows, function(row){

                    var entityId;

                    _.each( tables, function(table){
                        var tablePrefix = table.hashName + '_';
                        var tablePrefixLen = tablePrefix.length;
                        var componentData = { schemaId:table.schemaId };
                        var attrName;
                        for( var c in row ){
                            if( c.indexOf(tablePrefix) == 0 ){
                                attrName = c.substring(tablePrefixLen)
                                if( attrName == 'entity_id' && !entityId ){
                                    entityId = row[c];
                                }
                                else if( attrName == '_additional' ){
                                    componentData = _.extend( componentData, JSON.parse( row[c] ) );
                                }
                                else
                                    componentData[ attrName ] = row[c];
                                // if( attrName == 'title' ){
                                //     log.debug('title ' + row[c] );
                                // }
                            }
                        }

                        if( componentData.id == null ){
                            // log.debug('not adding component ' + table.schemaId + ' ' + componentData.id + ' for entity ' + entityId ); 
                            return;
                        }
                        
                        var component = self.registry.createComponent( componentData, null, createComponentOptions );
                        // log.debug('created component ' + table.schemaId + ' ' + componentData.id + ' for entity ' + entityId );

                        // print_ins( component );
                        // process.exit();
                        self._addComponent( component, odgnEntity.Entity.toEntity(entityId) );
                    });
                });
                
                return callback( null, self );     
            });
        });

        
    },
});




EntitySet.create = function(attrs, storage, registry, options){
    options = options || {};
    var Model = options.Model || exports.Model;
    var result = new EntitySet(attrs,{storage:storage, registry:registry});
    return result;
};


if( typeof module !== 'undefined' && module.exports ){
    module.exports = { create:EntitySet.create, Model:EntitySet };
}