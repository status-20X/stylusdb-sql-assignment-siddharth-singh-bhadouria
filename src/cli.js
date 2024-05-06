const readline = require('readline');
const { executeSELECTQuery, executeINSERTQuery, executeDELETEQuery } = require('./queryExecutor');

const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout
});

rl.setPrompt('SQL> ');
console.log('SQL Query Engine CLI. Enter your SQL commands, or type "exit" to quit.');

rl.prompt();

rl.on('line', async (line) => {
    if (line.toLowerCase() === 'exit') {
        rl.close();
        return;
    }

    try {
        let result;
        if (line.trim().toUpperCase().startsWith('SELECT')) {
            result = await executeSELECTQuery(line);
        } else if (line.trim().toUpperCase().startsWith('INSERT')) {
            result = await executeINSERTQuery(line);
        } else if (line.trim().toUpperCase().startsWith('DELETE')) {
            result = await executeDELETEQuery(line);
        } else {
            throw new Error('Unsupported SQL command');
        }

        console.log('Result:', result);
    } catch (error) {
        console.error('Error:', error.message);
    }

    rl.prompt();
}).on('close', () => {
    console.log('Exiting SQL CLI');
    process.exit(0);
});
