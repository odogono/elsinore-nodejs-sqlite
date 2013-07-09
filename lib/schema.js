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
        "id":{ "type": "integer", "orderPriority":50 },
        "entity_id":{ "type": "integer", "orderPriority":49 }, // reference to /schema/entity
        "component_id":{ "type": "integer", "orderPriority":48 }, // reference to /schema/component
        "_status":{ "type":"integer", "orderPriority":-46 },
        "_created_at":{ "type":"string", "format":"date-time", "orderPriority":-47 },
        "_created_by":{ "type":"integer", "orderPriority":-48 },
        "_updated_at":{ "type":"string", "format":"date-time", "orderPriority":-49 },
        "_updated_by":{ "type":"integer", "orderPriority":-50  },
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
        var properties = odgnEntitySchema.getProperties( [schemaId,'/schema/component_defaults'] );
        var tableName = 'tbl_' + odgnEntitySchema.titleFromSchema( schemaId ).toLowerCase();

        result.push("INSERT INTO ");
        result.push( tableName );
        result.push(" (");

        result.push( _.pluck(properties,'name').join(',') );

        result.push(") VALUES (");

        result.push( _.repeat('?', properties.length, ',') );
        result.push(")");

        return result.join('');      
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