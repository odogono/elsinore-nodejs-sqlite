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
        "status":{ "type":"integer", "default":0 },
        "component_bf":{ "type":"string", "description":"a bitfield of component defs on this component" },
        "created_at":{ "type":"string", "format":"date-time", "default":"${now}" },
        "created_by":{ "type":"integer", "default":0 },
        "updated_at":{ "type":"string", "format":"date-time", "default":"${now}" },
        "updated_by":{ "type":"integer", "default":0 },
    },
    "tbl_name":"tbl_entity",
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
        "type":{ "type":"integer", "default":0 },
        "created_at":{ "type":"string", "format":"date-time" },
        "created_by":{ "type":"integer" },
        "updated_at":{ "type":"string", "format":"date-time" },
        "updated_by":{ "type":"integer" },
    },
    "tbl_name":"tbl_component_def",
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
        // "component_id":{ "type": "integer" }, // reference to /schema/component
        "_additional":{ "type":"string" }, // stores additional fields as a json object
        "_status":{ "type":"integer", "default":0 },
        "_created_at":{ "type":"string", "format":"date-time", "default":"${now}" },
        "_created_by":{ "type":"integer", "default":0 },
        "_updated_at":{ "type":"string", "format":"date-time", "default":"${now}" },
        "_updated_by":{ "type":"integer", "default":0  },
    },
    "propertyPriorities":{
        "id":50,
        "entity_id":49,
        "component_id":48,
        "_additional":-45,
        "_status":-46,
        "_created_at":-47,
        "_created_by":-48,
        "_updated_at":-49,
        "_updated_by":-50,
    },
    "primaryKey":"id",
    "persist":false
};


var odgnEntitySchema;

/**
*   Tables which get created on startup
*/
exports.schemaTables = {
    entity: schemaEntity,
    componentDef: schemaComponentDef,
    componentDefaults: schemaComponentDefaults
};

exports.setup = function( odgnEntity ){
    odgnEntitySchema = odgnEntity;
}

var SqlStatement = Backbone.Model.extend({
    parse: function( resp, options ){
        if( !resp )
            return resp;
        if( _.isArray(resp) ){
            var result = {};
            for( var i in resp ){
                var property = resp[i];
                result[ property.name ] = _.isUndefined(property['value']) ? 
                            (_.isUndefined(property['default']) ? null : this._resolvePropertyValue( property['default']) ) :
                            property[value];
            }
            result.names = _.pluck(resp,'name');
            resp = result;
        }
        return resp;
    },

    _resolvePropertyValue: function( value ){
        if( value == '${now}' ){
            return new Date().toISOString();
        }
        return value;
    },

    getValues: function(){
        var self = this, names = this.get('names');
        if( !names )
            return [];

        return _.map( names, function(name){
            return self.get(name);
        });
    }
});

module.exports = _.extend( module.exports, {

    /**
     * Returns a sql table name from a schema
     * 
     * @param  {[type]} schema  [description]
     * @param  {[type]} options [description]
     * @return {[type]}         [description]
     */
    _getTableName: function( schema, options ){
        if( schema.tbl_name ){
            return schema.tbl_name;
        }
        var slug = schema.id.split('/');
        slug.shift();
        slug = slug.join('_').toLowerCase();
        // var slug = odgnEntitySchema.titleFromSchema( schema.id ).toLowerCase();
        var tableName = 'tbl_' + /*(options.isComponent ? 'com_':'') +*/ slug;
        return tableName;
    },

    /**
     * Registers a schema
     * 
     * @param  {[type]} schema [description]
     * @return {[type]}        [description]
     */
    register: function( schema ){
        return odgnEntitySchema.addSchema( schema.id, schema );
    },

    /**
     * Returns a SqlStatement that can be used to check the existence of a table
     * @param  {[type]} schema [description]
     * @return {[type]}        [description]
     */
    toCheckTableExists: function( schema, options ){
        options = options || {};
        var schemaId = _.isString(schema) ? schema : schema.id;
        var result = new SqlStatement();
        var schema = odgnEntitySchema.getSchema( schemaId );
        var tableName = this._getTableName(schema,options);

        result.set({
            'table_name': tableName,
            'sql': [ "SELECT name FROM sqlite_master WHERE type='table' AND name='", tableName, "' LIMIT 1;"].join('')});

        return result;
    },


    _statementForSchema: function( schemaId, options ){
        var sql = [], first = true;

        var coSchema = odgnEntitySchema.getSchema( schemaId, {combine:true} ); // TODO : tidy this
        var schema = odgnEntitySchema.getSchema( schemaId, {combine:false} );
        var properties = odgnEntitySchema.getProperties( schemaId, options );
        var primaryKey = schema.primaryKey || coSchema.primaryKey;

        // if( options && options.debug ){
            // print_ins( schema );
            // process.exit();
        // }
        // var schema = odgnEntitySchema.getSchema( schemaId );
        // var primaryKey = schema.primaryKey;
        // var properties = odgnEntitySchema.getProperties( schemaId );
        var tableName = this._getTableName(schema,options);

        var result = new SqlStatement();

        result.set({
            'table_name': tableName
        });

        // exclude the primary key from properties
        if( primaryKey ) {
            properties = _.reject( properties, function(prop){
                return prop.name == 'id';
            });
            result.set('primary_key', primaryKey );
        }

        var propertyNames = _.pluck(properties,'name');

        result.set( result.parse(properties) );
        result.set( 'property_names', propertyNames );

        return result;
    },

    toInsert: function( schemaId, options ){
        options = options || {};
        var sql = [], first = true;
        var result = this._statementForSchema( schemaId, options );
        var tableName = result.get('table_name');

        var propertyNames = result.get('property_names');

        if( options.debug ){
            print_ins( result.get('primary_key') );
            print_ins( propertyNames );
        }

        sql.push("INSERT INTO ");
        sql.push( tableName );
        sql.push(" (");

        sql.push( propertyNames.join(',') );

        sql.push(") VALUES (");

        sql.push( _.repeat('?', propertyNames.length, ',') );
        sql.push(")");

        result.set('sql', sql.join('') );


        return result;
    },

    toSelect: function( schemaId, options ){
        var sql = [], first = true;
        options = options || {};
        var result = this._statementForSchema( schemaId, options );
        var tableName = result.get('table_name');
        var parameters = [];

        sql.push("SELECT * FROM ");
        sql.push( tableName );

        if( options.where ){
            sql.push(' WHERE ');
            var wherePairs = _.pairs( options.where );
            wherePairs = _.map( wherePairs, function(pair){ 
                result.set(pair[0], pair[1]);
                parameters.push( pair[1] );
                return pair[0] + '=?';
            });
            sql.push( wherePairs.join(', ') );
        }

        // sql.push(" WHERE id=?" );
        result.set('sql', sql.join('') );
        result.set( 'parameters', parameters );

        return result;
    },

    toUpdate: function( schemaId, attrs, options ){
        var sql = [], first = true;
        var result = this._statementForSchema( schemaId, options );
        var tableName = result.get('table_name');

        sql.push("UPDATE ");
        sql.push( tableName );

        sql.push(" SET " );
        var setVals = [];
        for( var k in attrs ){
            setVals.push( k + "=?" );
            // sql.push( k );
            // sql.push("='");
            // sql.push( attrs[k] );
            // sql.push("' ");
        }
        sql.push( setVals.join(', ') );
        // log.debug( attrs.join('=') );

        sql.push(' WHERE id=?' );

        result.set('sql', sql.join('') );

        return result;
    },

    toDelete: function( schemaId, options ){
        var physicallyDelete = options && options.physicalDelete;

        var result = new SqlStatement();

        result.set({
            'table_name': tableName,
            'sql': sql.join('') });

        return result;
    },

    /**
     * Returns a SqlStatement which encapsulates a create operation
     * 
     * @param  {[type]} schemaId [description]
     * @return {[type]}          [description]
     */
    toCreate: function( schemaId, options ){
        options = options || {};
        var sql = [], first = true;
        var schema = schemaId, schemas = schemaId;
        // if( _.isArray(schema) ){
        //     schema = schema[0];
        // }


        var coSchema = odgnEntitySchema.getSchema( schema, {combine:true} ); // TODO : tidy this
        var schema = odgnEntitySchema.getSchema( schema, {combine:false} );
        var comSchema = odgnEntitySchema.getSchema( schemas );
        var properties = odgnEntitySchema.getProperties( schemas, options );
        var result = new SqlStatement();
        var primaryKey = schema.primaryKey || coSchema.primaryKey;

        // if( options.isComponent ){
            // log.debug("HO HEY " + primaryKey );
            // print_ins( coSchema ) ;
            // process.exit();
        // }

        var tableName = this._getTableName(schema,options);

        sql.push('CREATE TABLE IF NOT EXISTS ');
        sql.push( tableName );
        sql.push('( ');

        for( var i in properties ){
            var property = properties[i];
            if( !first ){
                sql.push( ', ');
            }
            first = false;
            sql.push( property.name );
            switch( property.type ){
                case 'string':
                    if( property.format == 'date-time' )
                        sql.push(' DATETIME DEFAULT CURRENT_TIMESTAMP');
                    else
                        sql.push(' STRING');

                    break;
                case 'integer':
                    if( property['default'] )
                        sql.push(' INTEGER DEFAULT ' + property['default'] );
                    else
                        sql.push(' INTEGER');
                    break;
            }
            if( primaryKey && primaryKey == property.name ){
                sql.push(' PRIMARY KEY');
            }
            if( property.unique ){
                sql.push(' UNIQUE');
            }
        }

        sql.push(");");

        result.set({
            'table_name': tableName,
            'sql': sql.join('') 
        });

        return result;
    }
});
