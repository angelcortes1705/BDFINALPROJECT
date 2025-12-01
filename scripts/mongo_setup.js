// Run with: node mongo_setup.js (requires mongodb driver and env MONGO_URI)
const { MongoClient } = require('mongodb');
const uri = process.env.MONGO_URI || 'mongodb://localhost:27017';
(async()=>{
  const client = new MongoClient(uri);
  await client.connect();
  const db = client.db('frauddb');

  // Users
  await db.collection('users').createIndex({email:1},{unique:true});
  await db.collection('users').createIndex({user_id:1},{unique:true});

  // Clients
  await db.collection('clients').createIndex({client_id:1});
  await db.collection('clients').createIndex({'identifiers.CURP':1});
  await db.collection('clients').createIndex({'addresses.location':'2dsphere'});

  // Accounts
  await db.collection('accounts').createIndex({account_id:1});
  await db.collection('accounts').createIndex({client_id:1});

  // Transactions
  await db.collection('transactions').createIndex({account_id:1, txn_time:-1});
  await db.collection('transactions').createIndex({txn_time:-1});
  await db.collection('transactions').createIndex({device_id:1});
  await db.collection('transactions').createIndex({ip_id:1});
  await db.collection('transactions').createIndex({merchant:1});
  await db.collection('transactions').createIndex({amount:1});

  // Alerts
  await db.collection('alerts').createIndex({created_at:-1});
  await db.collection('alerts').createIndex({severity:1});
  await db.collection('alerts').createIndex({status:1});
  await db.collection('alerts').createIndex({related_txns:1}); // multikey

  // Cases
  await db.collection('cases').createIndex({case_id:1});
  await db.collection('cases').createIndex({owner_user_id:1});

  console.log('Indexes created');
  await client.close();
})();
