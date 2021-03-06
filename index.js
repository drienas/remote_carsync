if (process.env.NODE_ENV !== 'production') {
  require('dotenv').config();
}
const mongoose = require(`mongoose`);

const MONGO_DB = process.env.MONGO_DB || null;

if (!MONGO_DB) {
  console.error(`NOT_ALL_ENV_SET`);
  process.exit(0);
}

const rmq = require('./rmq');
const mongoUrl = `mongodb://${MONGO_DB}/cars`;
let iConnection, iChannel, iQueue;
let Car;

const ack = (msg) => iChannel.ack(msg, false);
const nackError = (msg) => iChannel.nack(msg, false, true);
const nack = (msg) => iChannel.nack(msg, false, false);

const carSchema = new mongoose.Schema(
  {
    fzg_id: {
      type: String,
      required: true,
      index: true,
    },
  },
  { timestamps: true, strict: false }
);

const setUpMongoDbConnection = () => {
  console.log(`Connecting to MongoDB @ ${mongoUrl}`);
  mongoose.connect(mongoUrl, {
    useNewUrlParser: true,
    useUnifiedTopology: true,
    useCreateIndex: true,
  });

  mongoose.connection.on('error', (err) => {
    console.error(err);
    process.exit(0);
  });

  mongoose.connection.on('disconnected', (msg) => {
    console.error(msg);
    process.exit(0);
  });
};

const mdbUpsertDataset = async (data) =>
  new Promise(async (resolve, reject) => {
    try {
      let fzg_id = data.fzg_id;
      let existing = await Car.findOne({ fzg_id });
      if (existing) {
        let _id = existing._id;
        await Car.updateOne(
          {
            _id,
          },
          data
        );
      } else {
        await new Car(data).save();
      }
      let succ = await iChannel.publish(
        'carbridge_x',
        'update.r.bc',
        Buffer.from(JSON.stringify({ id: fzg_id }))
      );
      console.log(fzg_id);
      resolve(succ);
    } catch (err) {
      reject(err);
    }
  });

const rmqConsumeQueue = (channel, queue) => {
  console.log(`Ready to handle queued cars from rabbitmq bridge`);
  channel.consume(
    queue.queue,
    async (msg) => {
      try {
        let data;
        try {
          data = JSON.parse(msg.content.toString());
        } catch (err) {
          console.error(err);
          nack(msg);
        }
        if (!data.fzg_id) {
          console.error(`No proper FZG_ID set`);
          nack(msg);
        }
        let success = await mdbUpsertDataset(data);
        if (success) ack(msg);
        else throw `Could not handle car`;
      } catch (err) {
        console.error(err);
        nackError(msg);
      }
    },
    { noAck: false }
  );
};

const startSync = async () => {
  console.log(`Up and ready to sync cars to mongodb`);

  iConnection = await rmq.connect();
  iChannel = await rmq.initExchangeChannel(iConnection, 'carbridge_x');

  iQueue = await rmq.initQueue(
    iChannel,
    'carbridge_x',
    'CarBridge',
    ['update.dsg.detail'],
    {
      prefetch: true,
      prefetchCount: 3,
    }
  );

  rmqConsumeQueue(iChannel, iQueue);
};

(async () => {
  try {
    setUpMongoDbConnection();
    mongoose.connection.on('connected', (err) => {
      console.log(`Connected to MongoDB`);
      if (err) {
        console.error(err);
        process.exit(0);
      }
      Car = mongoose.model('Car', carSchema);
      startSync();
    });
  } catch (err) {
    console.error(err);
  }
})();
