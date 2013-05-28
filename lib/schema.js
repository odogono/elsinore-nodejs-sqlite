/**
 * Describes a table containing all the entities
 * 
 * @type {Object}
 */
var schemaEntity = {
    "id":"/schema/entity",
    "title":"entity",
    "type":"object",
    "properties":{
        "id":{ "type":"integer" },
        "status":{ "type":"integer" },
        "created_at":{ "type":"string", "format":"date-time" },
        "created_by":{ "type":"integer" },
        "updated_at":{ "type":"string", "format":"date-time" },
        "updated_by":{ "type":"integer" },
    },
    "primaryKey":"id"
};

/**
 * A Table of all the registered components
 * 
 * @type {Object}
 */
var schemaComponentDef = {
    "id":"/schema/component_def",
    "type":"object",
    "properties":{
        "id":{ "type":"integer" },
        "uri":{ "type":"string", "format":"uri" }, // schema uri
        "schema":{ "type":"string" "format":"json" }, // string re
        "status":{ "type":"integer" },
        "created_at":{ "type":"string", "format":"date-time" },
        "created_by":{ "type":"integer" },
        "updated_at":{ "type":"string", "format":"date-time" },
        "updated_by":{ "type":"integer" },    
    },
    "primaryKey":"id"
};


/**
 * Describes the default structure of a component table. Combined with
 * the component fields to create the table
 * 
 * @type {Object}
 */
var schemaComponentDefaults = {
    "id":"/schema/component_defaults",
    "type":"object",
    "properties":{
        "id":{ "type", "integer", "orderPriority":3 },
        "entity_id":{ "type", "integer", "orderPriority":2 }, // reference to /schema/entity
        "component_id":{ "type", "integer", "orderPriority":1 }, // reference to /schema/component
        "_status":{ "type":"integer", "orderPriority":-1 },
        "_created_at":{ "type":"string", "format":"date-time", "orderPriority":-2 },
        "_created_by":{ "type":"integer", "orderPriority":-3 },
        "_updated_at":{ "type":"string", "format":"date-time", "orderPriority":-4 },
        "_updated_by":{ "type":"integer", "orderPriority":-5  },
    }
}


module.exports = function(odgnEntity, options){

    var Schema = odgnEntity.Schema;

    return {
        schemas:{
            entity: schemaEntity,
            componentDef: schemaComponentDef
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