import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { BatchWriteCommand } from "@aws-sdk/lib-dynamodb";
import dotenv from "dotenv";
import { redisManagerInstance } from "./redis.js";
import { Keyword } from "../models/keyword.js";

dotenv.config();

const client = new DynamoDBClient({ region: "us-east-1" });
const redisBloomClient = await redisManagerInstance.getRedisBloomClient();
export const sourceLinkedInSearchKeywordTable =
  "source-linkedin-search-keywords";

export async function dynamoDBAddWithRetry(
  items,
  tableName,
  bloomSourceLinkedIn,
  maxRetries = 5,
  backoffFactor = 200,
) {
  let retries = 0;
  let unprocessedItems = items;

  while (unprocessedItems.length > 0 && retries < maxRetries) {
    try {
      const params = {
        RequestItems: {
          [tableName]: unprocessedItems?.map((item) => ({
            PutRequest: { Item: item },
          })),
        },
      };

      const response = await client.send(new BatchWriteCommand(params));

      const successfullyProcessedItems = unprocessedItems?.filter(
        (item) =>
          !response.UnprocessedItems ||
          !response.UnprocessedItems[tableName]?.find(
            (req) => req.PutRequest.Item.id === item.id,
          ),
      );

      for (const item of successfullyProcessedItems) {
        await redisBloomClient.bf.add(bloomSourceLinkedIn, item.id);
      }

      if (
        response.UnprocessedItems &&
        Object.keys(response.UnprocessedItems)?.length > 0
      ) {
        unprocessedItems = response.UnprocessedItems[tableName].map(
          (req) => req.PutRequest.Item,
        );
        console.log(`Retrying ${unprocessedItems?.length} unprocessed items.`);
      } else {
        unprocessedItems = [];
      }
    } catch (error) {
      if (error?.name === "ProvisionedThroughputExceededException") {
        retries++;
        const waitTime = backoffFactor * Math.pow(2, retries);
        console.error(`Throughput exceeded. Retrying in ${waitTime} ms...`);
        await new Promise((resolve) => setTimeout(resolve, waitTime));
      } else {
        console.error(`Error adding batch:`, error?.message);
      }
    }

    if (retries >= maxRetries) {
      console.error(
        `Max retries reached. Unprocessed items: ${unprocessedItems?.length}.`,
      );
      break;
    }
  }

  if (unprocessedItems.length > 0) {
    console.error(
      `Failed to process ${unprocessedItems.length} items after max retries.`,
    );
  }
}

export async function queryDynamoDBWithPagination(
  keyConditions,
  filterConditions,
  totalCount,
) {
  let results = [];
  let lastKey = null;

  try {
    while (results.length < totalCount) {
      let query = Keyword.query(keyConditions).limit(1000);

      for (const [key, value] of Object.entries(filterConditions)) {
        if (typeof value === "object") {
          for (const [operator, val] of Object.entries(value)) {
            query = query.filter(key)[operator](val);
          }
        } else {
          query = query.filter(key).eq(value);
        }
      }
      if (lastKey) query = query.startAt(lastKey);

      const data = await query.exec();
      results = results.concat(data);
      lastKey = data.lastKey || null;

      if (!lastKey) break;
    }

    return results?.slice(0, totalCount);
  } catch (error) {
    console.error("Error querying DynamoDB:", error?.message);
    return [];
  }
}
