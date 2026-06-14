import {
  DynamoDBClient,
  ScanCommand,
  GetItemCommand,
  PutItemCommand,
  DeleteItemCommand,
} from "@aws-sdk/client-dynamodb";
import { marshall, unmarshall } from "@aws-sdk/util-dynamodb";
import { config } from "../config.ts";
import {
  mergePolicy,
  type AppCreateInput,
  type AppPolicy,
  type AppRecord,
  type AppUpdateInput,
} from "../types/app.ts";
import { type AppsRepository } from "./types.ts";

export class DynamodbAppsRepository implements AppsRepository {
  private client: DynamoDBClient;
  private table: string;

  constructor() {
    const ddb = config.database.dynamodb;
    this.table = ddb.table;
    this.client = new DynamoDBClient({
      region: ddb.region,
      ...(ddb.endpoint ? { endpoint: ddb.endpoint } : {}),
      ...(ddb.accessKeyId && ddb.secretAccessKey
        ? {
            credentials: {
              accessKeyId: ddb.accessKeyId,
              secretAccessKey: ddb.secretAccessKey,
            },
          }
        : {}),
    });
  }

  async list(): Promise<AppRecord[]> {
    const result = await this.client.send(
      new ScanCommand({ TableName: this.table }),
    );
    return (result.Items ?? [])
      .map((item) => this.itemToApp(unmarshall(item)))
      .sort((a, b) => a.id.localeCompare(b.id));
  }

  async findById(id: string): Promise<AppRecord | null> {
    const result = await this.client.send(
      new GetItemCommand({
        TableName: this.table,
        Key: marshall({ id }),
      }),
    );
    if (!result.Item) return null;
    return this.itemToApp(unmarshall(result.Item));
  }

  async create(input: AppCreateInput): Promise<AppRecord> {
    const existing = await this.findById(input.id);
    if (existing) throw new Error("App already exists");

    const record: AppRecord = {
      id: input.id,
      key: input.key,
      secret: input.secret,
      enabled: input.enabled ?? true,
      policy: mergePolicy(input.policy),
    };

    await this.client.send(
      new PutItemCommand({
        TableName: this.table,
        Item: marshall(this.appToItem(record), { removeUndefinedValues: true }),
        ConditionExpression: "attribute_not_exists(id)",
      }),
    );

    return record;
  }

  async update(id: string, input: AppUpdateInput): Promise<AppRecord> {
    const existing = await this.findById(id);
    if (!existing) throw new Error("App not found");

    const record: AppRecord = {
      id,
      key: input.key ?? existing.key,
      secret: input.secret ?? existing.secret,
      enabled: input.enabled ?? existing.enabled,
      policy: mergePolicy({
        ...existing.policy,
        ...input.policy,
        limits: { ...existing.policy.limits, ...input.policy?.limits },
        features: { ...existing.policy.features, ...input.policy?.features },
        channels: { ...existing.policy.channels, ...input.policy?.channels },
        webhooks: input.policy?.webhooks ?? existing.policy.webhooks,
      }),
    };

    await this.client.send(
      new PutItemCommand({
        TableName: this.table,
        Item: marshall(this.appToItem(record), { removeUndefinedValues: true }),
      }),
    );

    return record;
  }

  async delete(id: string): Promise<void> {
    await this.client.send(
      new DeleteItemCommand({
        TableName: this.table,
        Key: marshall({ id }),
        ConditionExpression: "attribute_exists(id)",
      }),
    );
  }

  async close(): Promise<void> {
    this.client.destroy();
  }

  private appToItem(app: AppRecord): Record<string, unknown> {
    return {
      id: app.id,
      key: app.key,
      secret: app.secret,
      enabled: app.enabled,
      policy: JSON.stringify(app.policy),
    };
  }

  private itemToApp(item: Record<string, unknown>): AppRecord {
    const policyRaw = item.policy;
    let policy: AppPolicy;
    if (typeof policyRaw === "string") {
      policy = JSON.parse(policyRaw) as AppPolicy;
    } else if (policyRaw && typeof policyRaw === "object") {
      policy = policyRaw as AppPolicy;
    } else {
      throw new Error(`Invalid policy for app ${item.id}`);
    }

    return {
      id: String(item.id),
      key: String(item.key),
      secret: String(item.secret),
      enabled: item.enabled !== false,
      policy,
    };
  }
}
