const fs = require('fs');
const csv = require('csv-parser');
const { parse } = require('json2csv');

function readCSV(filePath) {
    const results = [];

    return new Promise((resolve, reject) => {
        fs.createReadStream(filePath)
            .pipe(csv())
            .on('data', (data) => results.push(data))
            .on('end', () => {
                resolve(results);
            })
            .on('error', (error) => {
                reject(error);
            });
    });
}


async function writeCSV(filename, data) {
    const csv = parse(data);
    try {
        fs.writeFileSync(filename, csv);
        console.log(`Data successfully written to ${filename}`);
    } catch (error) {
        console.error(`Error writing CSV to ${filename}:`, error);
    }
}

module.exports ={readCSV,writeCSV};