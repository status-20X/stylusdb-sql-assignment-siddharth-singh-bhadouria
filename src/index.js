const { parseQuery } = require('./queryParser');
const readCSV = require('./csvReader');

function performInnerJoin(data, joinData, joinCondition, fields, table) {
    return data.flatMap(mainRow => {
        const matchedJoinRows = joinData.filter(joinRow => {
            const mainValue = mainRow[joinCondition.left.split('.')[1]];
            const joinValue = joinRow[joinCondition.right.split('.')[1]];
            return mainValue === joinValue;
        });

        return matchedJoinRows.map(joinRow => {
            return fields.reduce((acc, field) => {
                const [tableName, fieldName] = field.split('.');
                acc[field] = tableName === table ? mainRow[fieldName] : joinRow[fieldName];
                return acc;
            }, {});
        });
    });
}

function performLeftJoin(data, joinData, joinCondition, fields, table) {
    const leftJoinedData = data.flatMap(mainRow => {
        const matchedJoinRows = joinData.filter(joinRow => {
            const mainValue = mainRow[joinCondition.left.split('.')[1]];
            const joinValue = joinRow[joinCondition.right.split('.')[1]];
            return mainValue === joinValue;
        });

        if (matchedJoinRows.length === 0) {
            return [createResultRow(mainRow, null, fields, table, true)];
        }
        return matchedJoinRows.map(joinRow => createResultRow(mainRow, joinRow, fields, table, true));
    });
    return leftJoinedData;
}

function createResultRow(mainRow, joinRow, fields, table, includeAllMainFields) {
    const resultRow = {};
    if (includeAllMainFields) {
        Object.keys(mainRow || {}).forEach(key => {
            const prefixedKey = `${table}.${key}`;
            resultRow[prefixedKey] = mainRow ? mainRow[key] : null;
        });
    }
    fields.forEach(field => {
        const [tableName, fieldName] = field.includes('.') ? field.split('.') : [table, field];
        resultRow[field] = tableName === table && mainRow ? mainRow[fieldName] : joinRow ? joinRow[fieldName] : null;
    });
    return resultRow;
}

function performRightJoin(data, joinData, joinCondition, fields, table) {
    const rowStructure = data.length > 0 ? Object.keys(data[0]).reduce((acc, key) => {
        acc[key] = null;
        return acc;
    }, {}) : {};
    let rightJoinedData = joinData.map(joinRow => {
        const mainRowMatch = data.find(mainRow => {
            const mainValue = getValueFromGivenRow(mainRow, joinCondition.left);
            const joinValue = getValueFromGivenRow(joinRow, joinCondition.right);
            return mainValue === joinValue;
        });
        const mainRowToUse = mainRowMatch || rowStructure;
        return createResultRow(mainRowToUse, joinRow, fields, table, true);
    });
    return rightJoinedData;
}

function getValueFromGivenRow(row, compoundFieldName) {
    const [tableName, fieldName] = compoundFieldName.split('.');
    return row[`${tableName}.${fieldName}`] || row[fieldName];
}

function applyGroupBy(data, groupByFields, aggregateFunctions) {
    const groupResult = {};
    data.forEach(row => {
        const key = groupByFields.map(field => row[field]).join('-');
        if (!groupResult[key]) {
            groupResult[key] = { count: 0, sums: {}, mins: {}, maxes: {} };
            groupByFields.forEach(field => groupResult[key][field] = row[field]);
        }
        groupResult[key].count += 1;
        aggregateFunctions.forEach(func => {
            const match = /(\w+)\((\w+)\)/.exec(func);
            if (match) {
                const [, aggregateFunc, aggregateField] = match;
                const value = parseFloat(row[aggregateField]);
                switch (aggregateFunc.toUpperCase()) {
                    case 'SUM':
                        groupResult[key].sums[aggregateField] = (groupResult[key].sums[aggregateField] || 0) + value;
                        break;
                    case 'MIN':
                        groupResult[key].mins[aggregateField] = Math.min(groupResult[key].mins[aggregateField] || value, value);
                        break;
                    case 'MAX':
                        groupResult[key].maxes[aggregateField] = Math.max(groupResult[key].maxes[aggregateField] || value, value);
                        break;
                }
            }
        });
    });
    return Object.values(groupResult).map(group => {
        const finalGroup = {};
        groupByFields.forEach(field => finalGroup[field] = group[field]);
        aggregateFunctions.forEach(func => {
            const match = /(\w+)\((\*|\w+)\)/.exec(func);
            if (match) {
                const [, aggregateFunc, aggregateField] = match;
                switch (aggregateFunc.toUpperCase()) {
                    case 'SUM':
                        finalGroup[func] = group.sums[aggregateField];
                        break;
                    case 'MIN':
                        finalGroup[func] = group.mins[aggregateField];
                        break;
                    case 'MAX':
                        finalGroup[func] = group.maxes[aggregateField];
                        break;
                    case 'COUNT':
                        finalGroup[func] = group.count;
                        break;
                }
            }
        });
        return finalGroup;
    });
}

async function executeSELECTQuery(query) {
    try {
        const { fields, table, whereClauses, joinType, joinTable, joinCondition, groupByFields, hasAggregateWithoutGroupBy, orderByFields, limit, isDistinct } = parseQuery(query);
        let data = await readCSV(`${table}.csv`);
        if (joinTable && joinCondition) {
            const joinData = await readCSV(`${joinTable}.csv`);
            switch (joinType.toUpperCase()) {
                case 'INNER':
                    data = performInnerJoin(data, joinData, joinCondition, fields, table);
                    break;
                case 'LEFT':
                    data = performLeftJoin(data, joinData, joinCondition, fields, table);
                    break;
                case 'RIGHT':
                    data = performRightJoin(data, joinData, joinCondition, fields, table);
                    break;
                default:
                    throw new Error(`Unsupported join type`);
            }
        }
        let filteredData = whereClauses.length > 0 ? data.filter(row => whereClauses.every(clause => evaluateCondition(row, clause))) : data;
        let groupData = filteredData;
        if (hasAggregateWithoutGroupBy) {
            const output = {};
            fields.forEach(field => {
                const match = /(\w+)\((\*|\w+)\)/.exec(field);
                if (match) {
                    const [, aggregateFunc, aggregateField] = match;
                    switch (aggregateFunc.toUpperCase()) {
                        case 'COUNT':
                            output[field] = filteredData.length;
                            break;
                        case 'SUM':
                            output[field] = filteredData.reduce((acc, row) => acc + parseFloat(row[aggregateField]), 0);
                            break;
                        case 'AVG':
                            output[field] = filteredData.reduce((acc, row) => acc + parseFloat(row[aggregateField]), 0) / filteredData.length;
                            break;
                        case 'MIN':
                            output[field] = Math.min(...filteredData.map(row => parseFloat(row[aggregateField])));
                            break;
                        case 'MAX':
                            output[field] = Math.max(...filteredData.map(row => parseFloat(row[aggregateField])));
                            break;
                    }
                }
            });
            return [output];
        } else if (groupByFields) {
            groupData = applyGroupBy(filteredData, groupByFields, fields);
            let orderOutput = groupData;
            if (orderByFields) {
                orderOutput = groupData.sort((a, b) => {
                    for (let { fieldName, order } of orderByFields) {
                        if (a[fieldName] < b[fieldName]) return order === 'ASC' ? -1 : 1;
                        if (a[fieldName] > b[fieldName]) return order === 'ASC' ? 1 : -1;
                    }
                    return 0;
                });
            }
            if (limit !== null) {
                orderOutput = orderOutput.slice(0, limit);
            }
            if (isDistinct) {
                groupData = [...new Map(groupData.map(item => [fields.map(field => item[field]).join('|'), item])).values()];
            }
            return groupData;
        } else {
            let orderOutput = groupData;
            if (orderByFields) {
                orderOutput = groupData.sort((a, b) => {
                    for (let { fieldName, order } of orderByFields) {
                        if (a[fieldName] < b[fieldName]) return order === 'ASC' ? -1 : 1;
                        if (a[fieldName] > b[fieldName]) return order === 'ASC' ? 1 : -1;
                    }
                    return 0;
                });
            }
            if (limit !== null) {
                orderOutput = orderOutput.slice(0, limit);
            }
            if (isDistinct) {
                orderOutput = [...new Map(orderOutput.map(item => [fields.map(field => item[field]).join('|'), item])).values()];
            }
            return orderOutput.map(row => {
                const selectedRow = {};
                fields.forEach(field => {
                    selectedRow[field] = row[field];
                });
                return selectedRow;
            });
        }
    } catch (error) {
        throw new Error(`Error executing query: ${error.message}`);
    }
}

function evaluateCondition(row, clause) {
    let { field, operator, value } = clause;
    if (row[field] === undefined) {
        throw new Error(`Invalid field`);
    }
    const rowValue = parsingValue(row[field]);
    let conditionValue = parsingValue(value);
    if (operator === 'LIKE') {
        const regexPattern = '^' + value.replace(/%/g, '.*').replace(/_/g, '.') + '$';
        const regex = new RegExp(regexPattern, 'i');
        return regex.test(row[field]);
    }
    switch (operator) {
        case '=': return rowValue === conditionValue;
        case '!=': return rowValue !== conditionValue;
        case '>': return rowValue > conditionValue;
        case '<': return rowValue < conditionValue;
        case '>=': return rowValue >= conditionValue;
        case '<=': return rowValue <= conditionValue;
        default: throw new Error(`Unsupported operator: ${operator}`);
    }
}

function parsingValue(value) {
    if (value === null || value === undefined) {
        return value;
    }
    if (typeof value === 'string' && ((value.startsWith("'") && value.endsWith("'")) || (value.startsWith('"') && value.endsWith('"')))) {
        value = value.substring(1, value.length - 1);
    }
    if (!isNaN(value) && value.trim() !== '') {
        return Number(value);
    }
    return value;
}

async function func() {
    const query = 'SELECT DISTINCT student.name FROM student INNER JOIN enrollment ON student.id = enrollment.student_id';
    const result = await executeSELECTQuery(query);
    console.log("Result", result);
}
func();
module.exports = executeSELECTQuery;
