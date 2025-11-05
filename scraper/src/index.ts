import { Kafka } from "kafkajs";
import dotenv from "dotenv";
import { fetchTrendingRepos, lastSeen } from "./read-topics";
dotenv.config();

const kafka = new Kafka({
  clientId: "github-trending-collector",
  brokers: [process.env.KAFKA_BROKER!],
});
const producer = kafka.producer();
const topic = process.env.KAFKA_TOPIC!;


async function main() {
  await producer.connect();
  console.log("Connected to Kafka producer");
  
  while (true) {
    try {
      const repos = await fetchTrendingRepos();
      const newOnes = repos.filter((repo) => !lastSeen.has(repo.id));

      if (newOnes.length) {
        await producer.send({
          topic,
          messages: newOnes.map((repo) => ({
            key: repo.id,
            value: JSON.stringify(repo),
          })),
        });
        
        console.log(`Sent ${newOnes.length} new trending repos to Kafka`);
        newOnes.forEach((repo) => lastSeen.add(repo.id));
        
        if (lastSeen.size > 1000) lastSeen.clear();
      } else {
        console.log("No new trending repos found");
      }

      await new Promise((r) => setTimeout(r, 30 * 60 * 1000));
      
    } catch (err) {
      console.error("Error scraping GitHub trending:", err);
      await new Promise((r) => setTimeout(r, 5 * 60 * 1000));
    }
  }
}

main().catch(console.error);