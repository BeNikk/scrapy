import { Kafka } from "kafkajs";
import { BigQuery } from "@google-cloud/bigquery";
import dotenv from "dotenv";
dotenv.config();

const kafka = new Kafka({
  clientId: "github-trending-worker",
  brokers: [process.env.KAFKA_BROKER!],
});
const consumer = kafka.consumer({ groupId: "bigquery-workers" });

const bigquery = new BigQuery({
  projectId: process.env.GOOGLE_CLOUD_PROJECT_ID!,
  keyFilename: process.env.GOOGLE_CLOUD_KEY_FILE!,
});

const dataset = bigquery.dataset(process.env.BQ_DATASET_ID!);
const table = dataset.table(process.env.BQ_TABLE_ID!);

interface GitHubRepo {
  id: string;
  name: string;
  owner: string;
  description: string;
  language: string;
  stars: number;
  forks: number;
  starsToday: number;
  url: string;
  scrapedAt: string;
}

function transformForBigQuery(repo: GitHubRepo) {
  return {
    repo_id: repo.id,
    repo_name: repo.name,
    owner: repo.owner,
    description: repo.description || null,
    language: repo.language || null,
    stars_count: repo.stars,
    forks_count: repo.forks,
    stars_today: repo.starsToday,
    url: repo.url,
    scraped_timestamp: new Date(repo.scrapedAt),
    inserted_at: new Date(),
  };
}

async function insertToBigQuery(repos: GitHubRepo[]) {
  try {
    const rows = repos.map(transformForBigQuery);
    
    console.log(`Inserting ${rows.length} repositories to BigQuery...`);
    console.log("Sample row:", JSON.stringify(rows[0], null, 2));
    
    const result: any = await table.insert(rows);
    const insertErrors = Array.isArray(result) ? result[0] : result;
    
    if (insertErrors && Array.isArray(insertErrors) && insertErrors.length > 0) {
      console.error("BigQuery insertion errors ");
      insertErrors.forEach((error: any, index: number) => {
        console.error(`Row ${index}:`, JSON.stringify(error, null, 2));
      });
      throw new Error(`Failed to insert ${insertErrors.length} rows to BigQuery`);
    } else {
      console.log(` Successfully inserted ${rows.length} repositories to BigQuery`);
    }
    
  } catch (error) {
    console.error("Error inserting to BigQuery:", error);
    throw error;
  }
}

async function main() {
  try {
    await consumer.connect();
    console.log("Connected to Kafka consumer");
    
    await consumer.subscribe({ 
      topic: process.env.KAFKA_TOPIC!,
      fromBeginning: true
    });
    
    console.log(`Subscribed to topic: ${process.env.KAFKA_TOPIC}`);
    
    await consumer.run({
      eachBatch: async ({ batch, resolveOffset, heartbeat }) => {
        console.log(`Processing batch of ${batch.messages.length} messages`);
        
        const repos: GitHubRepo[] = [];
        
        for (const message of batch.messages) {
          try {
            if (message.value) {
              const repoData = JSON.parse(message.value.toString());
              repos.push(repoData);
              
              resolveOffset(message.offset);
            }
          } catch (parseError) {
            console.error("Error parsing message:", parseError);
            resolveOffset(message.offset);
          }
          
          await heartbeat();
        }
        
        if (repos.length > 0) {
          try {
            await insertToBigQuery(repos);
            console.log("inserted to big query");
          } catch (bigQueryError) {
            console.error("Failed to insert batch to BigQuery:", bigQueryError);
            throw bigQueryError;
          }
        }
      },
      
      eachBatchAutoResolve: false,
    });
    
  } catch (error) {
    console.error("Consumer error:", error);
  }
}

process.on('SIGINT', async () => {
  console.log('Shutting down consumer...');
  await consumer.disconnect();
  process.exit(0);
});

process.on('SIGTERM', async () => {
  console.log('Shutting down consumer...');
  await consumer.disconnect();
  process.exit(0);
});

main().catch(console.error);