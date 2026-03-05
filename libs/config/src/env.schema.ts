import { z } from 'zod';

/**
 * Single source of truth for environment variables.
 * Validated at startup; missing required vars or invalid values throw ZodError.
 * Defaults allow local dev without a full .env.
 */
export const envSchema = z.object({
  /** Kafka broker list (comma-separated). Use localhost:9092 locally, kafka:19092 in Docker. */
  KAFKA_BROKERS: z.string().default('localhost:9092'),
  /** Silence KafkaJS v2 partitioner warning. Set to 1 to hide. */
  KAFKAJS_NO_PARTITIONER_WARNING: z.string().optional().default('1'),
  /** OpenSearch node URL. Use http://opensearch:9200 in Docker. */
  OPENSEARCH_NODE: z.string().default('http://localhost:9200'),
  /** Confluent Schema Registry URL. Use http://schema-registry:8081 in Docker. */
  SCHEMA_REGISTRY_URL: z.string().default('http://localhost:8081'),
  /** HTTP server port (api-gateway default 3000; set e.g. PORT=3001 for wikimedia-producer). */
  PORT: z
    .string()
    .default('3000')
    .transform((v) => Number.parseInt(v, 10)),
});

export type Env = z.infer<typeof envSchema>;

/** Parsed env — call once at app startup (e.g. when config is first loaded). */
function parseEnv(): Env {
  return envSchema.parse(process.env);
}

/**
 * Validated env, populated when any code imports from @app/config.
 * Ensures we never lose track of env vars and fail fast on invalid/missing required values.
 */
export const env: Env = parseEnv();
