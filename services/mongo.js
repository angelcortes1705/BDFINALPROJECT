const mongoose = require('mongoose');
module.exports = {
  connect: async (uri) => {
    await mongoose.connect(uri, { useNewUrlParser:true, useUnifiedTopology:true });
    console.log('Mongo connected');
  },
  mongoose
};