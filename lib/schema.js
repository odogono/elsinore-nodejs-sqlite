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
        "uri":{ "type":"string", "format":"uri", "unique":true }, // schema uri
        "schema":{ "type":"string", "format":"json" }, // string re
        "status":{ "type":"integer", "default":0 },
        "created_at":{ "type":"string", "format":"date-time", "default":"NOW()" },
        "created_by":{ "type":"integer", "default":0 },
        "updated_at":{ "type":"string", "format":"date-time", "default":"NOW()" },
        "updated_by":{ "type":"integer", "default":0 },
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
        "id":{ "type": "integer" },
        "entity_id":{ "type": "integer" }, // reference to /schema/entity
        "component_id":{ "type": "integer" }, // reference to /schema/component
        "_status":{ "type":"integer" },
        "_created_at":{ "type":"string", "format":"date-time" },
        "_created_by":{ "type":"integer" },
        "_updated_at":{ "type":"string", "format":"date-time" },
        "_updated_by":{ "type":"integer"  },
    },
    "propertyPriorities":{
        "id":50,
        "entity_id":49,
        "component_id":48,
        "_status":-46,
        "_created_at":-47,
        "_created_by":-48,
        "_updated_at":-49,
        "_updated_by":-50,
    },
    "persist":false
};


var odgnEntitySchema;

/**
*   Tables which get created on startup
*/
exports.schemaTables = {
    entity: schemaEntity,
    componentDef: schemaComponentDef
};

exports.setup = function( odgnEntity ){
    odgnEntitySchema = odgnEntity;
}


module.exports = _.extend( module.exports, {

    register: function( schema ){
        return odgnEntitySchema.addSchema( schema.id, schema );
    },

    toInsert: function( schemaId ){
        var result = [], first = true;
        var schema = odgnEntitySchema.getSchema( schemaId );
        var primaryKey = schema.primaryKey;
        var properties = odgnEntitySchema.getProperties( [schemaId,'/schema/component_defaults'] );
        var tableName = 'tbl_' + odgnEntitySchema.titleFromSchema( schemaId ).toLowerCase();

        // exclude the primary key
        if( primaryKey ) {
            properties = _.reject( properties, function(prop){
                return prop.name == 'id';
            });
        }

        var propertyNames = _.pluck(properties,'name');

        // log.debug('uh ' + JSON.stringify(schema) );
        result.push("INSERT INTO ");
        result.push( tableName );
        result.push(" (");

        result.push( propertyNames.join(',') );

        result.push(") VALUES (");

        result.push( _.repeat('?', propertyNames.length, ',') );
        result.push(")");

        var defaultValues = odgnEntitySchema.getDefaultValues( [schemaId,'/schema/component_defaults'] );
        if( primaryKey ) {
            delete defaultValues[ primaryKey ];
        }

        return {sql:result.join(''), properties:propertyNames, values:defaultValues}; 
    },

    toUpdate: function( schemaId, options ){
        var result = [], first = true;

        return result.join('');
    },

    toCreate: function( schemaId ){
        var result = [], first = true;
        var schema = odgnEntitySchema.getSchema( schemaId );
        var properties = odgnEntitySchema.getProperties( schemaId );

        var tableName = 'tbl_' + odgnEntitySchema.titleFromSchema( schemaId ).toLowerCase();

        result.push('CREATE TABLE IF NOT EXISTS ');
        result.push( tableName );
        result.push('( ');

        for( var i in properties ){
            var property = properties[i];
            if( !first ){
                result.push( ', ');
            }
            first = false;
            result.push( property.name );
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
            if( schema.primaryKey && schema.primaryKey == property.name ){
                result.push(' PRIMARY KEY');
            }
            if( property.unique ){
                result.push(' UNIQUE');
            }
        }

        result.push(");");

        return result.join('');
    }
});
