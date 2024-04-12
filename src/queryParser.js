function parseQuery(query) {
    query = query.trim();

    const limitRegex = /\sLIMIT\s(\d+)/i;
    const orderByRegex = /\sORDER BY\s(.+)/i;
    const groupByRegex = /\sGROUP BY\s(.+)/i;
    const selectRegex = /^SELECT\s(.+?)\sFROM\s(.+)/i;

    const limitMatch = query.match(limitRegex);

    let limit = null;
    if (limitMatch) {
        limit = parseInt(limitMatch[1]);
        query = query.replace(limitRegex,'')
    }

    const orderByMatch = query.match(orderByRegex);

    let orderByFields = null;
    if (orderByMatch) {
        orderByFields = orderByMatch[1].split(',').map(field => {
            const [fieldName, order] = field.trim().split(/\s+/);
            return { fieldName, order: order ? order.toUpperCase() : 'ASC' };
        });
        query = query.replace(orderByRegex, '');
    }

    
    const groupByMatch = query.match(groupByRegex);
    let selectPart, fromPart;

    const whereSplit = query.split(/\sWHERE\s/i);
    query = whereSplit[0];

    let whereClause = whereSplit.length > 1 ? whereSplit[1].trim() : null;
    if (whereClause && whereClause.includes('GROUP BY')) {
        whereClause = whereClause.split(/\sGROUP\sBY\s/i)[0].trim();
    }

    const joinSplit = query.split(/\s(INNER|LEFT|RIGHT) JOIN\s/i);
    selectPart = joinSplit[0].trim();

    const joinPart = joinSplit.length > 1 ? joinSplit[1].trim() : null;

    const selectMatch = selectPart.match(selectRegex);
    if (!selectMatch) {
        throw new Error('Invalid SELECT format');
    }

    const [, fields, rawTable] = selectMatch;
 
    let joinType ;
    let joinTable ;
    let joinCondition ;
    if (joinPart) {
        ( { joinType, joinTable, joinCondition } = parseJoinClause(query));
    }else{
        joinType=null;
        joinTable=null;
        joinCondition=null;
    }

    let whereClauses = [];
    if (whereClause) {
        whereClauses = parseWhereClause(whereClause);
    }

    const table = groupByMatch ? rawTable.split('GROUP BY')[0].trim() : rawTable.trim();

    const aggregateFunctionRegex = /\b(COUNT|SUM|AVG|MIN|MAX)\(.+?\)/i;
    const hasAggregateFunction = fields.match(aggregateFunctionRegex);

    let hasAggregateWithoutGroupBy = false;
    let groupByFields = null;
    
    if (groupByMatch) {
        groupByFields = groupByMatch[1].split(',').map(field => field.trim());
    }   
    if (hasAggregateFunction && !groupByMatch) {
        hasAggregateWithoutGroupBy = true;
    }

    return {
        fields: fields.split(',').map(field => field.trim()),
        table: table.trim(),
        whereClauses,
        joinType,
        joinTable,
        joinCondition,
        groupByFields,
        hasAggregateWithoutGroupBy,
        orderByFields,
        limit
    };
}

function parseWhereClause(whereString) {
    const conditionRegex = /(.*?)(=|!=|>|<|>=|<=)(.*)/;
    return whereString.split(/ AND | OR /i).map(conditionString => {
        const match = conditionString.match(conditionRegex);
        if (match) {
            const [, field, operator, value] = match;
            return { field: field.trim(), operator, value: value.trim() };
        }
        throw new Error('Invalid WHERE clause format');
    });
}

function parseJoinClause(query) {
    const joinRegex = /\s(INNER|LEFT|RIGHT) JOIN\s(.+?)\sON\s([\w.]+)\s*=\s*([\w.]+)/i;
    const joinMatch = query.match(joinRegex);

    if (joinMatch) {
        return {
            joinType: joinMatch[1].trim(),
            joinTable: joinMatch[2].trim(),
            joinCondition: {
                left: joinMatch[3].trim(),
                right: joinMatch[4].trim()
            }
        };
    }

    return {
        joinType: null,
        joinTable: null,
        joinCondition: null
    };
}

const query = 'SELECT id, name FROM student ORDER BY age DESC LIMIT 2';
const res = parseQuery(query)


module.exports = {parseQuery,parseJoinClause};
