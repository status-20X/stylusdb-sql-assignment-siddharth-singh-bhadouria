function parseSelectQuery(query) {
    try {
        query = query.trim();

        const limitRegex = /\sLIMIT\s(\d+)/i;
        const orderByRegex = /\sORDER BY\s(.+)/i;
        const groupByRegex = /\sGROUP BY\s(.+)/i;
        const selectRegex = /^SELECT\s(.+?)\sFROM\s(.+)/i;
        let isDistinct = false;
        if (query.toUpperCase().includes('SELECT DISTINCT')) {
            isDistinct = true;
            query = query.replace('SELECT DISTINCT', 'SELECT');
        }
        const limitMatch = query.match(limitRegex);

        let limit = null;
        if (limitMatch) {
            limit = parseInt(limitMatch[1], 10);
            query = query.replace(limitRegex, '');
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

        let joinType, joinTable, joinCondition;
        if (joinPart) {
            ({ joinType, joinTable, joinCondition } = parseJoinClause(query));
        } else {
            joinType = null;
            joinTable = null;
            joinCondition = null;
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
            limit,
            isDistinct
        };
    } catch (error) {
        throw new Error(`Query parsing error: ${error.message}`);
    }
}

function parseWhereClause(whereString) {
    const conditionRegex = /(.+?)(=|!=|>|<|>=|<=)(.+)/;
    return whereString.split(/ AND | OR /i).map(conditionString => {
        if (conditionString.includes(' LIKE ')) {
            const [field, pattern] = conditionString.split(/\sLIKE\s/i);
            return { field: field.trim(), operator: 'LIKE', value: pattern.trim().replace(/^'(.*)'$/, '$1') };
        } else {
            const match = conditionString.match(conditionRegex);
            if (match) {
                const [, field, operator, value] = match;
                return { field: field.trim(), operator, value: value.trim() };
            }
            throw new Error('Invalid WHERE clause format');
        }
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

function parseINSERTQuery(query) {
    const insertRegex = /INSERT\s+INTO\s+([^\s\(]+)\s*\(([^)]+)\)\s*VALUES\s*\(([^)]+)\)/i;
    const match = query.match(insertRegex)

    if (!match) {
        throw new error("Wrong INSERT INTO syntax")
    }

    const [, table, columns, values] = match;
    return {
        type: 'INSERT',
        table: table.trim(),
        columns: columns.split(',').map(column => column.trim()),
        values: values.split(',').map(value => value.trim())
    }
}

function parseDeleteQuery(query) {
    const deleteRegex = /DELETE FROM (\w+)( WHERE (.*))?/i;
    const match = query.match(deleteRegex);
    if (!match) {
        throw new Error("Wrong DELETE syntax.");
    }
    const [, table, , whereString] = match;
    let whereClauses = [];
    if (whereString) {
        whereClauses = parseWhereClause(whereString);
    }

    return {
        type: 'DELETE',
        table: table.trim(),
        whereClauses
    };
}

module.exports = { parseSelectQuery, parseJoinClause, parseINSERTQuery, parseDeleteQuery };
