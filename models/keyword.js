import dynamoose from "dynamoose";
import dotenv from "dotenv";

dotenv.config();
const ddb = new dynamoose.aws.ddb.DynamoDB({
  region: "us-east-1", //TODO -  Use .env or default to us-east-1
});

dynamoose.aws.ddb.set(ddb);

const keywordSchema = new dynamoose.Schema(
  {
    id: {
      type: String,
      hashKey: true,
    },
    search_type: {
      type: String,
      index: {
        name: "search_type-next_search_at-index",
        global: true,
        rangeKey: "next_search_at",
      },
    },
    search_key: {
      type: String,
    },
    next_search_at: {
      type: String,
    },
    last_search_at: {
      type: String,
    },
    is_fetched_before: {
      type: Boolean,
      default: false,
    },
    level: {
      type: Number,
    },
  },
  {
    timestamps: false,
  },
);

export const Keyword = dynamoose.model(
  "source-linkedin-search-keywords",
  keywordSchema,
  {
    create: false,
    waitForActive: false,
    update: false,
  },
);
