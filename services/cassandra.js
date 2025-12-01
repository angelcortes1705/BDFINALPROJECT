const cassandra = require('cassandra-driver');
let client;
module.exports = {
  connect: async (contactPoints=['127.0.0.1'], localDataCenter='datacenter1') => {
    client = new cassandra.Client({ contactPoints, localDataCenter, keyspace:'fraudks' });
    await client.connect();
    console.log('Cassandra connected');
    return client;
  },
  getClient: ()=>client
};