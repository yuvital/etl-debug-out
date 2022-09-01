

import { setTimeout } from 'timers/promises';
import mongodb from 'mongodb';
const ObjectID = mongodb.ObjectID;

(async function () {

    const MongoClient = mongodb.MongoClient;
    const mongoUri = 'mongodb://datalake:z42bVzxCOEqGvOlo@datalake-xrmh7.a.query.mongodb.net/rumble_lake?ssl=true&authSource=admin';
    const poolSize = 200;
    const dbName = mongoUri.split('/')[3].split('?')[0];
    const client = new MongoClient(mongoUri, { useNewUrlParser: true, useUnifiedTopology: true });    
    const from = new Date(new Date().getTime() - 3600000);
    //const match = { _id: ObjectID("61faa6689af48d000c3493c6") };
    const match =
    {
        docUpdatedAt: {
            $gte: new Date(from),
            $lte: new Date()
        }
    };

    console.log(process.version);
    console.log('calling mongo')

    await client.db(dbName).collection('competitions').aggregate([
        {
            $match: match
        },
        {
            "$out": {
                "s3": {
                    "bucket": 'data-warehouse-sync-from-mongo',
                    "region": "eu-central-1",
                    "filename": 'competitions/testing123s',
                    "format": true ? {
                        "name": "parquet",
                        "maxFileSize": "10GB",
                        "maxRowGroupSize": "100MB"
                    } : {
                        "name": "json.gz"
                    }
                }
            }
        }
    ]).toArray()

    while (true) {
        console.log('sleeping');
        await setTimeout(5000);
    }

})();