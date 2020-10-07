var AWS = require('aws-sdk');

async function connect(connectionString) {    
    let { Client } = require('pg');
    conn = new Client({
        connectionString: connectionString,
        connectionTimeoutMillis: 5000,
    });
    try {
        await conn.connect()
    } catch ( e ) {
        console.log(e)
    } 
}

async function disconnect() {
    try {    
        conn.end();
        return true;
    } catch ( e ) {
        console.log(e)
    }
    return false;
}

async function showRows() {
    let { rows } = await conn.query(`SELECT NOW() as now`);
    for (const row of rows) {
        console.log(row);
    }
    conn.end()
}

async function deleteAllRows(data) {
    var result = false;
    try {
        await conn.query(
            `delete from emails`
        );           
        result = true;
    } catch ( e ) {
        console.log(e)
    }
    return result
}

async function insertMultipleRows(data) {
    var result = false;
    try {
        await conn.query(
            `delete from emails`
        );   
        let { rows } = await conn.query(
            `insert into emails (email, created_at) values ` + data.map(d => `('` + d + `', now())`).join(',')
        );    
        result = true;
    } catch ( e ) {
        console.log(e)
    }
    return result
}

module.exports = { connect, showRows, insertMultipleRows, deleteAllRows, disconnect }