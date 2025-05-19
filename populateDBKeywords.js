import {
  dynamoDBAddWithRetry,
  sourceLinkedInSearchKeywordTable,
} from "./utils/dynamodb.js";
import {
  bloomSourceLinkedInKeyword,
  redisManagerInstance,
} from "./utils/redis.js";
import { fetchArtists } from "./utils/services.js";
import { createDBEntry, getISTTime } from "./utils/utils.js";

const redisBloomClient = await redisManagerInstance?.getRedisBloomClient();

const processAddNewKeywordsToDB = async (type) => {
  try {
    const artistsData = await fetchArtists(type);
    const fullNames = Object.keys(artistsData);
    const allKeywordEntries = [];
    let batchSize = 5;
    const subKeywordTemplates = Array.from(
      new Set([
        "",
        "AI",
        "AI Deepfake",
        "AI singing",
        "AI music",
        "AI Cover",
        "Deepfakes",
        "AI content",
        "AI remix",
        "AI voice",
        "AI generated",
        "deepfake AI",
        "fake AI voice",
        "cover",
        "remix",
        "mashup",
        "reaction",
        "official video",
        "short",
        "viral",
        "trending",
        "leaked",
        "exposed",
      ]),
    );

    for (const full_name of fullNames) {
      const keywords = artistsData[full_name];
      const searchTerms = Array.from(new Set([full_name, ...keywords]));

      for (const term of searchTerms) {
        for (const suffix of subKeywordTemplates) {
          const keyword = suffix ? `${term} ${suffix}` : term;
          const id = Buffer.from(`${keyword}-${type}`).toString("base64");
          const isIdAlreadyPresent = await redisBloomClient.bf.exists(
            bloomSourceLinkedInKeyword,
            id,
          );
          if (!isIdAlreadyPresent) {
            const keywordEntry = createDBEntry(keyword, type);
            allKeywordEntries.push(keywordEntry);
          }
        }
      }
    }
    for (let i = 0; i < allKeywordEntries.length; i += batchSize) {
      const batch = allKeywordEntries.slice(i, i + batchSize);
      await dynamoDBAddWithRetry(
        batch,
        sourceLinkedInSearchKeywordTable,
        bloomSourceLinkedInKeyword,
      );
    }
    console.log("All keywords have been added to DynamoDB successfully.");
  } catch (e) {
    console.error("Error:", e?.message);
  }
};

const main = async () => {
  while (true) {
    console.log("Starting new keywords db updateing process...");
    await processAddNewKeywordsToDB("keyword");

    const sevenDaysInMs = 1 * 24 * 60 * 60 * 1000;
    console.log(
      `${getISTTime()} Waiting for 24 hour before the next update...`,
    );
    await new Promise((resolve) => setTimeout(resolve, sevenDaysInMs));
  }
};

(async () => {
  await main();
})();
