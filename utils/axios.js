import axios from "axios";
import dotenv from "dotenv";

dotenv.config();

export async function createAxiosInstance() {
  return axios.create({
    timeout: 30000,
    headers: {
      "User-Agent":
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3",
      "Accept-Language": "en-US,en;q=0.9",
      Accept:
        "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
      Connection: "keep-alive",
      "X-JoJAPI-Key":
        "jk_WfU7Cf7886K4239erd6azdf4cW6cc1c2C9I0654Afd82Bd51B4yQKu23HC4c8bJb",
    },
  });
}
