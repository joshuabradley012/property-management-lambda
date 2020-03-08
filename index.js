const Database = require('./Database');
const { config } = require('./config');

exports.handler = async (event) => {
  const database = new Database(config);

  const rows = await database.query('SELECT * FROM properties');

  await database.close();
  return rows;
};
