const { google } = require('googleapis')
const properties = require('./properties.json')
const key = require('./auth.json')
const scopes = [properties.google.spreadsheets_scope];
var fs = require('fs');
var redshift = require('./redshift');
const aws = require("aws-sdk")
aws.config.update({ region: properties.aws.region });
var _secrets;


const secretsManager = new aws.SecretsManager({
	region : properties.region
});

async function getAwsSecrets() {
	try {
	  const data = await secretsManager.getSecretValue({
		SecretId: properties.aws.secret_name,
	  }).promise();
  
	  if (data) {
		if (data.SecretString) {			
			const secret = data.SecretString;
			const parsedSecret = JSON.parse(secret);
			return {
					google_api_private_key : Buffer.from(parsedSecret[properties.aws.secret_private_key], 'base64').toString('ascii'),
					redshift_connection_string : parsedSecret[properties.aws.secret_redshift_connection_string]
				}
		}
		return false
	  }
	} catch (error) {
	  console.log('Error retrieving secrets');
	  console.log(error);
	}
	return false
}

async function authorizeGoogleSheets(googleApiPrivateKey) {
	googleAuth = new google.auth.JWT(key.client_email, null, googleApiPrivateKey, scopes)
	let result = await new Promise((resolve, reject) => {
		googleAuth.authorize(function (err, tokens) {
			if (err) {
				reject(err)
			} else {
				console.log("Authenticated")
				resolve(true)
			}
		})
	})
	return result
}

async function getGoogleSheetsData() {
	let result = await new Promise((resolve, reject) => {
		try {
			let emails = new Array()
			const sheets = google.sheets({
				version: properties.google.spreadsheet_api_version,
				auth: googleAuth
			});
			sheets.spreadsheets.values.get({
				spreadsheetId: properties.google.spreadsheet_id,
				range: properties.google.spreadsheet_range,
			}, (err, res) => {
				if (err) reject(err)
				if (res.data.values) {
					resolve(res.data.values)
				} else {
					reject('Spreadsheets - No data found.')
				}
			});
		} catch (err) {
			reject(err)
		}
	})
	return result
}

function getDataToJson(data) {
	if (rows.length) {
		rows.map((row) => {
			emails.push(row[0])
		});
		resolve(emails)
	}
}

function getDataCsv(data) {
	var csv = rows.map(function(d){
		return d.join();
	}).join('\n');
}

async function sendDataToRedshift(connectionString, data) {
	let result = await new Promise((resolve, reject) => {
		redshift.connect(connectionString)
			.then(() => redshift.deleteAllRows())
			.then(() => redshift.insertMultipleRows(data))
			.then(() => redshift.disconnect())
			.then((result) => { if(result) resolve(true) })
			.catch((err) => { reject(err) })			
	})
	return result
};

exports.handler = async (event) => {		
	//var secrets = getAwsSecrets().promise()	
	//if(secrets) {						

		/*return getAwsSecrets()
		.then( (secrets) => {
			_secrets = secrets;
			authorizeGoogleSheets(key.private_key) 
			//authorizeGoogleSheets(_secrets['google_api_private_key']) 
		})*/
		return authorizeGoogleSheets(key.private_key) 
		.then(() => getGoogleSheetsData())
		.then((data) =>  {
				var connectionString = "redshift://leandro:B0ler019$@schildt.cn6gpsja7am1.sa-east-1.redshift.amazonaws.com:5439/spredsheet2";
				return sendDataToRedshift(connectionString, data)
				//return sendDataToRedshift(_secrets['redshift_connection_string'], data)
			}		
		).catch(error => console.log(error));
	//}
	return false
}

console.log('return ' + exports.handler())