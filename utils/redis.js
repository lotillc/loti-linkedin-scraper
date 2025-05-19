import dotenv from "dotenv";
import redis from "redis";

dotenv.config();
export const sourceLinkedinUsers = "source-linkedin:users";

export const bloomSourceLinkedInUsers = "bloom-source-linkedin:users";
export const bloomSourceLinkedInPostInfo = "bloom-source-linkedin:postInfo";
export const bloomSourceLinkedInKeyword = "bloom-source-liknedin:keyword";

export const sourceLinkedInProfile = "source-linkedin:profile";

class RedisManager {
  constructor() {
    if (!RedisManager.instance) {
      this.redisClient = redis.createClient({
        url: process.env.REDIS_URL,
      });
      this.redisClient.on("error", (error) => {
        console.error("Redis Client Error:", error);
      });

      this.redisBloomClient = redis.createClient({
        url: process.env.REDIS_BLOOM_URL,
      });
      this.redisBloomClient.on("error", (error) => {
        console.error("Redis Bloom Client Error:", error);
      });

      RedisManager.instance = this;
    }

    return RedisManager.instance;
  }

  async getRedisClient() {
    if (!this.redisClient.isOpen) {
      await this.redisClient.connect();
    }
    return this.redisClient;
  }

  async getRedisBloomClient() {
    if (!this.redisBloomClient.isOpen) {
      await this.redisBloomClient.connect();
    }
    return this.redisBloomClient;
  }
}

const redisManagerInstance = new RedisManager();
Object.freeze(redisManagerInstance);

export { redisManagerInstance };

// //TODO - This is for adding username from populateRedisUsers
export async function addProfileToRedis(usernames, redisKey) {
  const redisClient = await redisManagerInstance.getRedisClient();
  const redisBloomClient = await redisManagerInstance.getRedisBloomClient();
  const usernameRecommendationCount = await redisClient.sCard(redisKey);

  if (usernameRecommendationCount < 1000000) {
    for (const item of usernames) {
      const id = Buffer.from(`${item?.search_key || item}`).toString("base64");
      const isIdAlreadyPresent = await redisBloomClient.bf.exists(
        bloomSourceLinkedInKeyword,
        id,
      );
      if (!isIdAlreadyPresent) {
        let term = `${item?.search_key || item}`;
        await redisClient.sAdd(redisKey, term);
      }
    }
  }
}

// //TODO - Need this to add usernama to `source-linkedin:users`
export async function addUsernameToRedisAndBloom(username, redisKey) {
  if (!username) return;
  const redisClient = await redisManagerInstance.getRedisClient();
  const redisBloomClient = await redisManagerInstance.getRedisBloomClient();
  const usernameCount = await redisClient.sCard(redisKey);

  if (usernameCount < 1000000) {
    const isExists = await redisBloomClient.bf.exists(
      bloomSourceLinkedInUsers,
      username, //!Todo - It should be unique adentifier from linkedIn (id)
    );

    if (!isExists) {
      await redisBloomClient.bf.add(bloomSourceLinkedInUsers, username);
      await redisClient.sAdd(bloomSourceLinkedInUsers, username);
    }
  }
}

export const incrReqCount = async () => {
  const redisClient = await redisManagerInstance.getRedisClient();
  const today = new Date()
    .toISOString()
    .split("T")[0]
    .split("-")
    .reverse()
    .join("-");

  const incrKey = `request:linkedin:${today}`;
  await redisClient.incrBy(incrKey, 1);
};

export const incrReqType = async (type) => {
  const redisClient = await redisManagerInstance.getRedisClient();
  const today = new Date()
    .toISOString()
    .split("T")[0]
    .split("-")
    .reverse()
    .join("-");

  const incrKey = `request:linkedin:${type}:${today}`;
  await redisClient.incrBy(incrKey, 1);
};
