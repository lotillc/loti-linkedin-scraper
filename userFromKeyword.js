import {
  addProfileToRedis,
  addUsernameToRedisAndBloom,
  bloomSourceLinkedInPostInfo,
  redisManagerInstance,
  sourceLinkedInProfile,
  sourceLinkedinUsers,
} from "./utils/redis.js";
import { getUsers } from "./utils/services.js";
import { delay } from "./utils/delay.js";
import { getScrapedCount } from "./utils/redis.js";
import { delayTime } from "./config/config.js";
import {
  addUsernameToDynamoDb,
  getISTTime,
  popBatchFromRedis,
} from "./utils/utils.js";

const redisBloomClient = await redisManagerInstance.getRedisBloomClient();

export async function processPost(keyword) {
  try {
    // let [term, last_scraped, level] = keyword?.split(" | ");
    let term = keyword;
    let hasMorePages = true;
    let nextPageUrl = "";
    let retries = 0;
    let pageCount = 0;
    let maxRetries = 2;
    let insightfulMessages = [];
    let lastPageUrl = "";
    let profileNameBatch = []; //!Todo - For searching profiles keyword (profile).
    let usernameBatch = []; //!Todo - For future scraping user posts (user)
    let companyBatch = []; // !Todo - For scraping company posts (company).
    let locationBatch = []; // !Todo - For scraping location posts (location).

    while (hasMorePages) {
      try {
        const { posts, nextPage, error } = await getUsers(term);
        pageCount++;
        if (posts && posts?.length > 0) {
          for (const data of posts) {
            if (data?.username) {
              const idExists = await redisBloomClient.bf.exists(
                bloomSourceLinkedInPostInfo,
                data?.username, //!Todo - Username validation, should be unique adentifier from linkedIn
              );
              if (!idExists) {
                if (data?.username) {
                  await addUsernameToRedisAndBloom(
                    data?.username,
                    sourceLinkedinUsers,
                  );
                  usernameBatch.push(data?.username);
                }
                if (data?.profileName) {
                  profileNameBatch.push(
                    ...data?.profileName?.split(" "),
                    data?.profileName,
                  );
                }

                insightfulMessages.push({
                  success: true,
                  message: "Success",
                  term: keyword,
                });
              } else {
                insightfulMessages.push({
                  success: false,
                  message: "Already scraped post.",
                  term: keyword,
                });
              }
            } else {
              insightfulMessages.push({
                success: false,
                message: "No Media post",
                term: keyword,
              });
            }
          }
        } else {
          insightfulMessages.push({
            success: true,
            message: "Success",
            term: `${term}`,
          });
        }
        if (nextPage) {
          nextPageUrl = nextPage;
          lastPageUrl = nextPageUrl;
        } else {
          hasMorePages = false;
          break;
        }

        await delay(delayTime, 0);
        retries = 0;
      } catch (error) {
        if (
          !error?.message?.includes("429") &&
          !error?.message?.includes("404") &&
          !error?.message?.includes("403") &&
          !error?.message?.includes("504") &&
          !error?.message?.includes("503") &&
          !error?.message?.includes("502")
        ) {
          console.error(`Error fetching posts: ${error?.message}`);
        }

        retries++;
        if (retries >= maxRetries) {
          retries = 0;
          console.log(`Max retry reached. Aborting.`);
          insightfulMessages.push({
            success: false,
            message: "Max retry reached",
            term: keyword,
          });
          break;
        } else {
          await delay(delayTime, retries);
        }
      }
    }
    if (profileNameBatch?.length > 0) {
      //!Todo - For now not adding profile to dynamoDB directly
      const uniqueProfiles = [...new Set(profileNameBatch)];
      await addProfileToRedis(uniqueProfiles);
    }

    if (usernameBatch?.length > 0) {
      const uniqueUsername = [...new Set(usernameBatch)];
      await addUsernameToDynamoDb(uniqueUsername);
    }
    return insightfulMessages;
  } catch (error) {
    console.error(`Error processing posts: ${error?.message}`);
    return [
      {
        success: false,
        message: `${error?.message || "Unknown error"}`,
        term: keyword,
      },
    ];
  }
}

async function main() {
  try {
    const jitterDelay = Math.floor(Math.random() * (60 - 10 + 1) + 10) * 1000;
    console.log(`Starting post scraper in ${jitterDelay / 1000} seconds...`);
    await new Promise((resolve) => setTimeout(resolve, jitterDelay));

    while (true) {
      const scrapedCount = await getScrapedCount();
      if (scrapedCount > 200000) {
        console.log(
          `${getISTTime()} 200k daily scrapeing limit reached, sleeping for 1 hours`,
        );
        await new Promise((resolve) => setTimeout(resolve, 1 * 60 * 60 * 1000));
        continue;
      }
      let batchSize = 1;
      const batch = await popBatchFromRedis(sourceLinkedInProfile, batchSize);
      if (!batch || batch.length === 0) {
        console.log(
          `${getISTTime()}There is no batch to pop sleeping for 1 hours`,
        );
        await new Promise((resolve) => setTimeout(resolve, 1 * 60 * 60 * 1000));
        continue;
      }

      let successCount = 0;
      let failedCount = 0;
      let messageCounts = {};
      const keywordWiseLog = {};
      const results = await Promise.allSettled(
        batch?.map((term) => processPost(term)),
      );
      results?.forEach((result) => {
        if (result?.status === "fulfilled" && result?.value) {
          result?.value?.forEach(({ success, message, term }) => {
            if (success) successCount++;
            else failedCount++;
            if (!keywordWiseLog[term]) {
              keywordWiseLog[term] = {};
            }
            keywordWiseLog[term][message] =
              (keywordWiseLog[term][message] || 0) + 1;

            if (message) {
              messageCounts[message] = (messageCounts[message] || 0) + 1;
            }
          });
        } else if (result.status === "rejected") {
          failedCount++;
          const errorMessage = result?.reason?.message || "Unknown error";
          messageCounts[errorMessage] = (messageCounts[errorMessage] || 0) + 1;
          searchTerms.forEach((term) => {
            if (!keywordWiseLog[term]) {
              keywordWiseLog[term] = {};
            }

            keywordWiseLog[term][errorMessage] =
              (keywordWiseLog[term][errorMessage] || 0) + 1;
          });
        }
      });
      console.log("-".repeat(50));
      console.log("# userFromKeyword.js");
      Object.keys(messageCounts).forEach((message) => {
        console.log(`${message}: ${messageCounts[message]} `);
      });
      console.log(
        `Finished userFromKeyword.js scraper | Batch: ${batchSize} | ${getISTTime()}`,
      );
      successCount = 0;
      failedCount = 0;
    }
  } catch (error) {
    console.error("Error in post scraper main function", {
      message: error?.message,
    });
  }
}

(async () => {
  await main();
})();
