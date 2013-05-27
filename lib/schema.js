
/**
 * Default Entity Schema
 * @type {Object}
 */
var schemaEntity = {
    "id":"/schema/entity",
    "title":"entity",
    "type":"object",
    "properties":{
        "id":{ "type":"integer" },
        "_status":{ "type":"integer" },
        "_created_at":{ "type":"string", "format":"date-time" },
        "_created_by":{ "type":"integer" },
        "_updated_at":{ "type":"string", "format":"date-time" },
        "_updated_by":{ "type":"integer" },
    },
    "primaryKey":"id"
};


module.exports = function(odgnEntity, options){

    var Schema = odgnEntity.Schema;

    return {
        schemas:{
            entity: schemaEntity
        },

        register: function( schema ){
            return Schema.addSchema( schema.id, schema );
        },

        toSql: function( schemaId ){
            var result = [], first = true;
            var schema = Schema.getSchema( schemaId );
            var properties = Schema.getProperties( schemaId );

            var tableName = 'tbl_' + Schema.titleFromSchema( schemaId ).toLowerCase();

            result.push('CREATE TABLE IF NOT EXISTS ');
            result.push( tableName );
            result.push('( ');

            for( var key in properties ){
                var property = properties[key];
                if( !first ){
                    result.push( ', ');
                }
                first = false;
                result.push( key );
                switch( property.type ){
                    case 'string':
                        if( property.format == 'date-time' )
                            result.push(' DATETIME DEFAULT CURRENT_TIMESTAMP');
                        else
                            result.push(' STRING');

                        break;
                    case 'integer':
                        result.push(' INTEGER');
                        break;
                }
                if( schema.primaryKey && schema.primaryKey == key )
                    result.push(' PRIMARY KEY');
            }

            result.push(");");

            return result.join('');
        }
    };
};

/*Schema.toSql = function(schemaUri, options){

    var result = [];
    var s = Schema.env.findSchema(schemaUri);
    var properties = Schema.properties(schemaUri);
    
    console.log( s._attributes )

    var tableName = 'tbl_' + s._attributes.entityId;

    result.push('CREATE TABLE ');
    result.push( tableName );
    result.push('( ');

    for( var key in properties ){
        var property = properties[key];
        result.push( key );
        result.push( ", ");
    }

    result.push(");");
    return result.join("");
}//*/