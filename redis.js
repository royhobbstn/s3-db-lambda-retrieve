const redis = require('redis');
const bluebird = require('bluebird');

bluebird.promisifyAll(redis.RedisClient.prototype);
bluebird.promisifyAll(redis.Multi.prototype);


const createRedisClient = () => {
  const clientObj = redis.createClient(12548, 'redis-12548.c1.us-west-2-2.ec2.cloud.redislabs.com', { no_ready_check: true, password: process.env.REDIS_PWRD });

  clientObj.on('error', (err) => {
    console.log(`Error while creating redis client: ${err}`);
  });

  return clientObj;
};

const client = createRedisClient();

exports.client = client;
