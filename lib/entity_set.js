var Schema = require('./schema');

var EntitySet = odgnEntity.EntitySet.Model.extend({

    initialize: function( attrs, options ){
        odgnEntity.EntitySet.Model.prototype.initialize.apply( this, arguments );

        if( attrs.select ){
            log.debug('examining select ');
        }

    },

    isComponentOfInterest: function( componentDef ){
        log.debug('not interested in ' + componentDef.id );
        return false;
    },

    reload: function(callback){
        var self = this;
        var statement = Schema.toComponentSelect( this.get('select') );
        var tables = statement.get('_tables');
        var sql = statement.getSql();

        var limit = this.get('page_size') || 10;
        var offset = (this.get('page')-1) * this.get('page_size');


        var createComponentOptions = { instantiate:true, debug:true };
        sql = sql + ' LIMIT ' + limit + ' OFFSET ' + offset;

        // print_ins(sql); process.exit();
        
        // do a count on the returned rows first
        return this.storage.count( statement, null, function(err,count){

            var pageSize = self.get('page_size');
            self.set('page', Math.floor( count / pageSize ) + 1 );
            self.set('entity_count', count );
            self.set('page_count', count == 0 ? 1 : Math.ceil(count / pageSize) );

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
                            }
                        }
                        
                        var component = self.registry.createComponent( componentData, null, createComponentOptions );
                        // log.debug('created component for entity ' + entityId );

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