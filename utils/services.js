import { createAxiosInstance } from "./axios.js";
import { formatKeyword, logToFile } from "./utils.js";
import { keywords } from "./keywords.js";
import { incrReqCount, incrReqType } from "./redis.js";
import { delay } from "./delay.js";

export async function fetchArtists(mode = "") {
  try {
    const axiosInstance = await createAxiosInstance();
    const { data } = await axiosInstance.get(
      "https://api-agent.goloti.com/artists/keywords",
    );

    const artistKeywords = {};
    data?.data?.forEach((artist) => {
      let formattedKeywords = [];
      if (Array.isArray(artist?.keywords)) {
        formattedKeywords = artist?.keywords?.map((keyword) =>
          formatKeyword(keyword, mode),
        );
      } else if (artist?.keywords && typeof artist?.keywords === "object") {
        Object?.values(artist.keywords)?.forEach((keyArray) => {
          if (Array?.isArray(keyArray)) {
            formattedKeywords?.push(
              ...keyArray?.map((keyword) => formatKeyword(keyword, mode)),
            );
          }
        });
      }
      artistKeywords[String(artist.full_name)] = [
        ...formattedKeywords,
        ...formattedKeywords?.map((keyword) => keyword?.toLowerCase()),
        artist?.full_name.toLowerCase(),
        artist?.full_name,
      ];
    });
    return artistKeywords;
  } catch (error) {
    console.error("Error fetching artist data:", error?.message);
    console.log("Attempting to load local backup of artist keywords...");
    const { data } = keywords;
    const artistKeywords = {};

    data?.forEach((artist) => {
      artistKeywords[artist.full_name] = [
        ...artist.keywords.map((keyword) => formatKeyword(keyword, mode)),
        ...artist.keywords.map((keyword) =>
          formatKeyword(keyword, mode).toLowerCase(),
        ),
        artist.full_name.toLowerCase(),
        artist.full_name,
      ];
    });

    return artistKeywords;
  }
}

export async function getUsers(term) {
  const maxRetries = 1;
  const delayTime = 2000;
  let retries = 0;
  let posts = [];
  const [firstName, secondName] = term?.split(" ");
  while (retries < maxRetries) {
    let postUrl = `https://linkedin.jojapi.net/v1/search-peoples?firstName=${firstName}&lastName=${secondName || ""}`;
    try {
      const axiosInstance = await createAxiosInstance();
      const { data } = await axiosInstance.get(postUrl);
      await incrReqCount();
      const entries = data?.results;

      for (let entry of entries) {
        posts?.push({
          username: entry?.profile_id,
          profileName: entry?.name,
          profileUrl: entry?.profile_url,
          profileImage: entry?.profile_image,
          company: entry?.company,
          location: entry?.location,
        });
      }
      const nextPage = false;
      return { posts, nextPage: nextPage || false, error: "" };
    } catch (error) {
      retries++;
      await incrReqType("failure");
      if (
        !error?.message?.includes("429") &&
        !error?.message?.includes("404") &&
        !error?.message?.includes("403") &&
        !error?.message?.includes("504") &&
        !error?.message?.includes("503") &&
        !error?.message?.includes("502") &&
        !error?.message?.includes("422") &&
        !error?.message?.includes("414")
      ) {
        console.error(`Error fetching posts for ${term}`, {
          message: error.message,
        });
      }
      if (retries < maxRetries) {
        await delay(delayTime, retries);
      } else {
        if (!error?.message.includes("TypeError")) {
          return { posts: [], nextPage: false, error: error?.message };
        }
      }
    }
  }
}
