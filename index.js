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

const rentRollQuery = (dateString) => {
  const [year, month] = dateParts(dateString);
  return `
  SELECT SUM(records.amount) AS rentRoll
    FROM records
   WHERE MONTH(records.date) = ${month}
     AND YEAR(records.date) = ${year}
     AND records.type = 'Rent'
`}

const rentCollectedQuery = (dateString) => {
  const [year, month] = dateParts(dateString);
  return `
  SELECT SUM(entries.amount) AS rentCollected
    FROM entries
   WHERE MONTH(entries.date) = ${month}
     AND YEAR(entries.date) = ${year}
     AND entries.type = 'Rent'
`}

const lateFeesQuery = (dateString) => {
  const [year, month] = dateParts(dateString);
  return `
  SELECT SUM(entries.amount) AS lateFees
    FROM entries
   WHERE MONTH(entries.date) = ${month}
     AND YEAR(entries.date) = ${year}
     AND entries.type = 'Late Fee'
`}

const reimbursementsQuery = (dateString) => {
  const [year, month] = dateParts(dateString);
  return `
  SELECT SUM(entries.amount) AS reimbursements
    FROM entries
   WHERE MONTH(entries.date) = ${month}
     AND YEAR(entries.date) = ${year}
     AND entries.type = 'Reimbursement'
`}

const paymentTimelineQuery = (dateString) => {
  const [year, month] = dateParts(dateString);
  return `
  SELECT entries.date AS name,
         SUM(entries.amount) AS collected,
         (SELECT SUM(records.amount) AS rentRoll
            FROM records
           WHERE MONTH(records.date) = ${month}
             AND YEAR(records.date) = ${year}
             AND records.type = 'Rent'
         ) AS due
    FROM entries
   WHERE MONTH(entries.date) = ${month}
     AND YEAR(entries.date) = ${year}
     AND entries.type = 'Rent'
   GROUP
      BY entries.date
   ORDER
      BY entries.date
`}

const paymentSourcesQuery = (dateString) => {
  const [year, month] = dateParts(dateString);
  return `
  SELECT entries.source AS name,
         SUM(entries.amount) AS value
    FROM entries
   WHERE MONTH(entries.date) = ${month}
     AND YEAR(entries.date) = ${year}
     AND entries.type = 'Rent'
   GROUP
      BY entries.source
`}

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
`}

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
`}

exports.handler = async (event, context) => {
  let rows = {};
  const database = new Database(config);
  if (event.queryStringParameters) {
    if (event.queryStringParameters.table) {
      rows = await database.query('SELECT * FROM ??', [event.queryStringParameters.table]);
    }
    if (event.queryStringParameters.fields) {
      const date = event.queryStringParameters.date;
      const fields = event.queryStringParameters.fields.split(',');
      for (const field of fields) {
        if (field === 'earliest-record') {
          const result = await database.query(earliestRecord);
          rows.earliestRecord = result[0].earliestRecord;
        }
        if (field === 'rent-roll') {
          const result = await database.query(rentRollQuery(date));
          rows.rentRoll = result[0].rentRoll;
        }
        if (field === 'rent-collected') {
          const result = await database.query(rentCollectedQuery(date));
          rows.rentCollected = result[0].rentCollected;
        }
        if (field === 'late-fees') {
          const result = await database.query(lateFeesQuery(date));
          rows.lateFees = result[0].lateFees;
        }
        if (field === 'reimbursements') {
          const result = await database.query(reimbursementsQuery(date));
          rows.reimbursements = result[0].reimbursements;
        }
        if (field === 'payment-timeline') rows.paymentTimeline = await database.query(paymentTimelineQuery(date));
        if (field === 'payment-sources')  rows.paymentSources  = await database.query(paymentSourcesQuery(date));
        if (field === 'payment-progress') rows.paymentProgress = await database.query(paymentProgressQuery(date));
        if (field === 'outstanding-rent') rows.outstandingRent = await database.query(outstandingRentQuery(date));
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
