const Database = require('./Database');
const { config } = require('./config');

const dateParts = (dateString) => {
  let date = new Date(Date.now());
  if (dateString && Date.parse(dateString)) date = new Date(dateString);
  return date.toISOString().split('T')[0].split('-').map(val => parseInt(val));
}

const earliestRecord = `
  SELECT MIN(date) as earliestRecord
    FROM records
`

const paymentProgressQuery = (dateString) => {
  const [year, month] = dateParts(dateString);
  return `
  SELECT properties.id,
         properties.name,
         SUM(records.amount) AS rentRoll,
         SUM(entries.amount) AS collected,
         SUM(records.amount) - SUM(entries.amount) AS due
    FROM properties
    LEFT
    JOIN buildings
      ON buildings.propertyId = properties.id
    LEFT
    JOIN units
      ON units.buildingId = buildings.id
    LEFT
    JOIN tenants
      ON tenants.unitId = units.id
    LEFT
    JOIN people
      ON people.id = tenants.personId
    LEFT
    JOIN records
      ON records.personId = people.id
     AND MONTH(records.date) = ${month}
     AND YEAR(records.date) = ${year}
     AND records.type = 'Rent'
    LEFT
    JOIN entries
      ON entries.recordId = records.id
     AND MONTH(entries.date) = ${month}
     AND YEAR(entries.date) = ${year}
     AND entries.type = 'Rent'
   GROUP
      BY properties.id
  `
}

const outstandingRentQuery = (dateString) => {
  const [year, month] = dateParts(dateString);
  return `
  SELECT tenants.id,
         people.name,
         CASE
           WHEN SUM(records.amount) - SUM(entries.amount) = 0
           THEN 'Paid'
           ELSE 'Unpaid'
         END AS status,
         SUM(records.amount) - SUM(entries.amount) AS balance,
         SUM(entries.amount) AS paid,
         entries.source,
         entries.date AS lastPayment
    FROM tenants
    LEFT
    JOIN people
      ON people.id = tenants.personId
    LEFT
    JOIN records
      ON records.personId = people.id
     AND MONTH(records.date) = ${month}
     AND YEAR(records.date) = ${year}
     AND records.type = 'Rent'
    LEFT
    JOIN entries
      ON entries.recordId = records.id
     AND MONTH(entries.date) = ${month}
     AND YEAR(entries.date) = ${year}
     AND entries.type = 'Rent'
   GROUP
      BY tenants.id
  `
}

exports.handler = async (event, context) => {
  let rows = [];
  const database = new Database(config);
  if (event.queryStringParameters) {
    if (event.queryStringParameters.table) {
      rows = await database.query('SELECT * FROM ??', [event.queryStringParameters.table]);
    }
    if (event.queryStringParameters.get) {
      if (event.queryStringParameters.get === 'earliest-record') {
        rows = await database.query(earliestRecord);
      }
      if (event.queryStringParameters.get === 'payment-progress') {
        rows = await database.query(paymentProgressQuery(event.queryStringParameters.date));
      }
      if (event.queryStringParameters.get === 'outstanding-rent') {
        rows = await database.query(outstandingRentQuery(event.queryStringParameters.date));
      }
    }
  }
  await database.close();

  let response = {
    headers: {
      'Access-Control-Allow-Origin' : '*',
      'Access-Control-Allow-Headers':'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token',
      'Access-Control-Allow-Methods': 'DELETE, GET, HEAD, OPTIONS, PATCH, POST, PUT',
      'Access-Control-Allow-Credentials' : true,
      'Content-Type': 'application/json',
    },
    statusCode: 200,
    body: JSON.stringify(rows),
  };
  context.succeed(response);
};
