import { bloomSourceLinkedInKeyword, redisManagerInstance } from "./redis.js";
import crypto from "crypto";
import moment from "moment";
import fs from "fs";
import {
  dynamoDBAddWithRetry,
  sourceLinkedInSearchKeywordTable,
} from "./dynamodb.js";

export function generateSHA256Hash(link) {
  return crypto.createHash("sha256").update(link).digest("hex");
}

export function getISTTime() {
  const options = { timeZone: "Asia/Kolkata", hour12: true };
  return new Date().toLocaleString("en-IN", options);
}

export function convertToISTISOString(utcDateStr) {
  if (!utcDateStr) return "";
  const utcDate = new Date(utcDateStr);
  const istOffset = 5.5 * 60 * 60 * 1000;
  const istAdjustedDate = new Date(utcDate.getTime() + istOffset);
  return istAdjustedDate?.toISOString();
}

export function validateTweetObjectKeys(obj) {
  const requiredKeys = [
    "username",
    "hearts",
    "retweets",
    "comments",
    "tweetContent",
    "tags",
    "date_scrapped",
    "published_at",
    "postId",
    "is_processed",
    "id",
    "page_url",
    "type",
    "post_type",
    "image_url",
    "video_url",
  ];

  const missingKeys = requiredKeys.filter((key) => !(key in obj));

  if (missingKeys.length > 0) {
    return { valid: false, missingKeys };
  }

  if (
    (obj.type === "video" &&
      (!obj.video_url ||
        obj.image_url ||
        (obj.post_type !== "video" && obj.post_type !== "slide_show"))) ||
    (obj.type === "image" &&
      (!obj.image_url ||
        obj.video_url ||
        (obj.post_type !== "image" && obj.post_type !== "slide_show")))
  ) {
    return {
      valid: false,
      error: "Validation failed based on 'type' and 'post_type' conditions",
    };
  }

  return { valid: true };
}

export const generateId = (url) =>
  crypto.createHash("sha256").update(url).digest("hex");

export function formatKeyword(keyword, mode) {
  if (mode == "hashtag") {
    return keyword;
  }
  return keyword.replace(/([a-z])([A-Z])/g, "$1 $2");
}

export async function popBatchFromRedis(key, count) {
  try {
    const redisClient = await redisManagerInstance.getRedisClient();
    const items = await redisClient.sPop(key, count);
    if (!items || items.length === 0) {
      console.log("No items popped from Redis");
      return [];
    }

    return items;
  } catch (err) {
    console.error("Error popping items from Redis:", err?.message);
    throw err;
  }
}

export const logToFile = (data, logFile = "Logs.log") => {
  const logEntry = `---------------------------------------------------------------------\n${JSON.stringify(data, null, 2)}\n-----------------------------------------------------------------------\n`;
  fs.appendFileSync(logFile, logEntry);
};

export async function addToBloomFilter(key, value) {
  try {
    const redisBloomClient = await redisManagerInstance.getRedisBloomClient();
    const added = await Promise.race([
      redisBloomClient.bf.add(key, value),
      timeoutPromise(10000),
    ]);

    return added;
  } catch (error) {
    console.error(
      `Error adding value to Bloom filter: ${value}`,
      error?.message,
    );
  }
}

function timeoutPromise(ms) {
  return new Promise((_, reject) =>
    setTimeout(() => reject(new Error("Operation timed out")), ms),
  );
}

export async function sleepForRandomTime(
  baseHours = 0,
  minMinutes = 50,
  maxMinutes = 80,
) {
  const randomMinutes =
    minMinutes + Math.floor(Math.random() * (maxMinutes - minMinutes + 1));
  const totalSleepTime = baseHours * 60 + randomMinutes;

  const hours = Math.floor(totalSleepTime / 60);
  const minutes = totalSleepTime % 60;

  console.log(
    `[${getISTTime()}] More than 1M items in redis, sleeping for ${hours} hour(s) and ${minutes} minute(s)`,
  );

  await new Promise((resolve) =>
    setTimeout(resolve, totalSleepTime * 60 * 1000),
  );
}

export function createSearchQuery(term, lastScraped) {
  const currentDate = moment().toISOString();

  const sinceDate =
    lastScraped == "0000-00-00"
      ? moment().subtract(1, "years").toISOString()
      : moment(new Date(lastScraped)).toISOString();
  const encodedTerm = encodeURIComponent(term);
  const encodedSince = encodeURIComponent(
    `since:${moment(sinceDate).format("YYYY-MM-DD")}`,
  );
  const encodedUntil = encodeURIComponent(
    `until:${moment(currentDate).format("YYYY-MM-DD")}`,
  );

  return `${encodedTerm}%20${encodedSince}%20${encodedUntil}`;
}

export const createDBEntry = (term, type) => {
  const getRandomDateInRange = (startOffset, endOffset) => {
    const now = new Date();
    const startDate = new Date(now);
    startDate.setDate(now.getDate() + startOffset);
    const endDate = new Date(now);
    endDate.setDate(now.getDate() + endOffset);

    const randomTime =
      startDate.getTime() +
      Math.random() * (endDate.getTime() - startDate.getTime());
    return new Date(randomTime).toISOString();
  };
  return {
    id: Buffer.from(`${term}-${type}`).toString("base64"),
    is_fetched_before: false,
    level: 0,
    last_search_at: "0000-00-00",
    next_search_at: getRandomDateInRange(-5, 2),
    search_type: type,
    search_key: term,
  };
};

export async function addUsernameToDynamoDb(items) {
  try {
    let batchSize = 25;
    let validUsers = [];
    const redisBloomClient = await redisManagerInstance.getRedisBloomClient();
    for (const user of items) {
      const id = Buffer.from(`${user}-user`).toString("base64");
      const isIdAlreadyPresent = await redisBloomClient.bf.exists(
        bloomSourceLinkedInKeyword, //NOTE - Using same bloom for keyword and user
        id,
      );
      if (!isIdAlreadyPresent) {
        const keywordEntry = createDBEntry(user, "user");
        validUsers?.push(keywordEntry);
      }
    }

    for (let i = 0; i < validUsers?.length; i += batchSize) {
      const batch = validUsers?.slice(i, i + batchSize);
      await dynamoDBAddWithRetry(
        batch,
        sourceLinkedInSearchKeywordTable,
        bloomSourceLinkedInKeyword, //NOTE - Using same bloom for keyword and user
      );
    }
  } catch (error) {
    console.error("Error adding item to Redis and Bloom", { error });
  }
}
