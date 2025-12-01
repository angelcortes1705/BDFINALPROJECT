const dgraph = require('dgraph-js');
const grpc = require('grpc');

let dgraphClient;
module.exports = {
  connect: (target='127.0.0.1:9080') => {
    const clientStub = new dgraph.DgraphClientStub(target, grpc.credentials.createInsecure());
    dgraphClient = new dgraph.DgraphClient(clientStub);
    console.log('Dgraph connected');
    return dgraphClient;
  },
  client: ()=>dgraphClient
};
