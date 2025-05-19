import { redisManagerInstance } from "./redis.js";

class ProxyManager {
  constructor() {
    if (!ProxyManager.instance) {
      this.proxies = [];
      this.updateProxy();
      setInterval(() => this.updateProxy(), 10 * 60 * 1000);
      ProxyManager.instance = this;
    }
    return ProxyManager.instance;
  }

  async updateProxy() {
    try {
      const redisClient = await redisManagerInstance.getRedisClient();
      if (!redisClient.isOpen) {
        await redisClient.connect();
      }
      const proxyBase64 = await redisClient.get("proxies:linkedin-scraper");
      if (proxyBase64) {
        const proxyString = Buffer.from(proxyBase64, "base64").toString(
          "utf-8",
        );
        this.proxies = JSON.parse(proxyString);
        const timestamp = new Date().toLocaleString();
      } else {
        console.log("No proxy found in Redis");
      }
    } catch (error) {
      console.error("Error fetching proxy from Redis:", error?.message);
    }
  }

  async getRandomProxy() {
    if (this.proxies.length === 0) {
      await this.updateProxy();
    }
    if (this.proxies.length === 0) return null;
    const randomIndex = Math.floor(Math.random() * this.proxies.length);
    return this.proxies[randomIndex];
  }
}

const instance = new ProxyManager();

export default instance;
