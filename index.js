const { google } = require('googleapis')
const key = require('./auth.json')
const properties = require('./properties.json')
const aws = require("aws-sdk")
aws.config.update({ region: properties.aws.region });
const firehose = new aws.Firehose();
const scopes = [properties.google.spreadsheets_scope];

const auth = new google.auth.JWT(key.client_email, null, key.private_key, scopes)

const secretsManager = new aws.SecretsManager({
	region : properties.region
});

async function getAwsGoogleApiSecret() {
	try {
	  const data = await secretsManager.getSecretValue({
		SecretId: properties.aws.secret_name,
	  }).promise();
  
	  if (data) {
		if (data.SecretString) {
			console.log('teste')
		  const secret = data.SecretString;
		  const parsedSecret = JSON.parse(secret);
		  return parsedSecret['private_key']
		}
		return false
	  }
	} catch (error) {
	  console.log('Error retrieving secrets');
	  console.log(error);
	}
	return false
}

async function authorizeGoogleSheets() {
	let result = await new Promise((resolve, reject) => {
		auth.authorize(function (err, tokens) {
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
				auth
			});
			sheets.spreadsheets.values.get({
				spreadsheetId: properties.google.spreadsheet_id,
				range: properties.google.spreadsheet_range,
			}, (err, res) => {
				if (err) reject(err)
				const rows = res.data.values
				if (rows.length) {
					rows.map((row) => {
						emails.push(row[0])
					});
					resolve(emails)
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

function getDataPagesToFirehose(data) {
	// Each PutRecordBatch request supports up to 500 records.
	let pages = Array()
	let totalPages = Math.ceil(data.length / 500)
	let startPos = 0
	let endPos = 500
	for (let i = 0; i < totalPages; i++) {
		let dataPage = data.slice(startPos, endPos)
		pages.push(dataPage)
		startPos += 500
		endPos += 500
	}
	return pages
}

async function sendDataPagesToFirehose(pages) {
	let result = await new Promise((resolve, reject) => {
		const promises = []
		pages.map((page) => {
			promises.push(putRecordsFirehose(page))
		})
		Promise.all(promises)
			.then(response => resolve(response))
			.catch(error => reject(error))
	})
	return result
}

async function putRecordsFirehose(page) {
	let result = await new Promise((resolve, reject) => {
		let records = Array()
		for (const row of page) {
			records.push({ Data: Buffer.from(JSON.stringify({'email': row})) })
		}
		firehose.putRecordBatch({
			DeliveryStreamName: properties.aws.firehose_delivery_stream_name,
			Records: records
		}, (err, data) => {
			if (err) {
				console.log(err, err.stack);
				reject(false);
			} else {
				console.log("success | put " + records.length + " records");					
				resolve(true);
			}
		})
	})
	return result
}

exports.handler = async (event) => {

	var googleApiPrivateKey = await getAwsGoogleApiSecret()
	if(googleApiPrivateKey) {
		console.log('private_key ' + googleApiPrivateKey)		
		return googleApiPrivateKey
	} 
	return false


/*

	return authorizeGoogleSheets()
		.then(() => getGoogleSheetsData())
		.then(data => getDataPagesToFirehose(data))
		.then(pages => {
			return sendDataPagesToFirehose(pages)
		})
		.catch(error => console.log(error));
*/
}