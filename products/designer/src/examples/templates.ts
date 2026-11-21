import { PROTOCOL_VERSION } from "../version";
import type {
  Connection,
  Configuration,
  DataField,
  DataSchema,
  FlowComponent,
  PackageDocument,
} from "../model";

/** The three supported v11 business paths. Keep these ids stable: they are
 * also the ids used by the browser fixture and by the acceptance ledger. */
export type JobTemplateKind =
  | "batch-incremental"
  | "cdc-mirror"
  | "orders-payments-shops-iceberg";

export interface TemplateConnectionInput {
  id: string;
  driver: string;
  resource: string;
  credentialReference?: string;
}

export interface TemplateOptions {
  kind: JobTemplateKind;
  packageId: string;
  label: string;
  sourceConnection: TemplateConnectionInput;
  targetConnection: TemplateConnectionInput;
  sourceTable: string;
  targetTable: string;
  changeSequence: string;
  keyColumn: string;
  cdcProvider: "postgresql" | "mysql" | "mariadb";
  cdcSlot: string;
  cdcPublication: string;
  cdcServerId: number;
  appendOnly: boolean;
  paymentsConnection?: TemplateConnectionInput;
  paymentsTopic: string;
  shopsConnection?: TemplateConnectionInput;
  shopsTable: string;
  icebergCatalog: TemplateConnectionInput;
  icebergStorage: TemplateConnectionInput;
  icebergNamespace: string;
  icebergTable: string;
  schedule?: TemplateSchedule;
  environment?: {
    id: string;
    revision: string;
  };
  parameters?: Record<string, unknown>;
}

export interface TemplateSchedule {
  id: string;
  frequency: "Once" | "Interval" | "Daily" | "Weekly" | "Monthly";
  startAt: string;
  enabled: boolean;
  intervalSeconds?: number;
  timeZone?: string;
  localTime?: string;
  weekdays?: number[];
  dayOfMonth?: number;
}

export interface TemplatePublishOptions {
  schedules: TemplateSchedule[];
  /** Sent as the explicit immutable environment binding at publication. */
  environment?: { id: string; revision: string };
  /** Sent separately from the package so Native can bind it at publication. */
  parameters?: Record<string, unknown>;
}

export interface BuiltTemplate {
  document: PackageDocument;
  publish: TemplatePublishOptions;
}

const field = (
  name: string,
  type: string,
  nullable = false,
  keyOrder?: number,
): DataField => ({
  name,
  type,
  nullable,
  ...(keyOrder === undefined ? {} : { keyOrder }),
});

const schema = (fields: DataField[]): DataSchema => ({
  version: PROTOCOL_VERSION,
  fields,
});

const connection = ({
  id,
  driver,
  resource,
  credentialReference,
}: TemplateConnectionInput): Connection => ({
  id,
  driver,
  resource,
  ...(credentialReference ? { credentialReference } : {}),
});

const node = (
  id: string,
  component: string,
  config: Configuration,
): FlowComponent => ({ id, component, config });

const edge = (
  from: string,
  to: string,
  output = 0,
  input = 0,
) => ({ from, to, output, input });

const idPart = (value: string, fallback: string) => {
  const normalized = value.trim().replace(/[^A-Za-z0-9_-]+/g, "-");
  return normalized.replace(/^-+|-+$/g, "").slice(0, 120) || fallback;
};

const base = (
  options: TemplateOptions,
  connections: Connection[],
  dataFlows: PackageDocument["dataFlows"],
  tasks: PackageDocument["controlFlow"]["tasks"],
  constraints: PackageDocument["controlFlow"]["constraints"] = [],
): PackageDocument => ({
  version: PROTOCOL_VERSION,
  id: idPart(options.packageId, "universaldata-job"),
  // Runtime environment values are sent as explicit publication parameters;
  // they are intentionally not copied into the immutable Package document.
  parameters: {},
  connections,
  controlFlow: { tasks, constraints },
  dataFlows,
  layout: {
    version: PROTOCOL_VERSION,
    label: options.label.trim() || options.packageId,
    nodes: dataFlows.flatMap((flow, flowIndex) =>
      flow.components.map((component, index) => ({
        flow: flow.id,
        id: component.id,
        x: 70 + (index % 3) * 320,
        y: 120 + flowIndex * 420 + Math.floor(index / 3) * 220,
      })),
    ),
    viewports: [],
  },
});

const publish = (options: TemplateOptions): TemplatePublishOptions => ({
  schedules: options.schedule ? [options.schedule] : [],
  ...(options.environment
    ? {
        environment: {
          id: options.environment.id,
          revision: options.environment.revision,
        },
      }
    : {}),
  ...(options.parameters ? { parameters: options.parameters } : {}),
});

function batchIncremental(options: TemplateOptions): BuiltTemplate {
  const sourceSchema = schema([
    field(options.changeSequence, "int64", false, 1),
    field(options.keyColumn, "int64", false, 2),
    field("amount", "decimal128", true),
  ]);
  const connections = [
    connection(options.sourceConnection),
    connection(options.targetConnection),
  ];
  const source = node("orders", "sql.source", {
    connection: options.sourceConnection.id,
    table: options.sourceTable,
    schema: sourceSchema,
    batchSize: 512,
    incremental: {
      mode: "composite",
      keys: [options.changeSequence, options.keyColumn],
      after: [0, 0],
      unique: true,
      monotonic: true,
    },
    schemaPolicy: { policy: "compatible" },
  });
  const target = node("finance", "sql.destination", {
    connection: options.targetConnection.id,
    table: options.targetTable,
    mode: "upsert",
    keys: [1],
    delivery: "receipt",
    provisionReceipts: true,
  });
  const document = base(
    options,
    connections,
    [{ id: "main", components: [source, target], paths: [edge("orders", "finance")] }],
    [{ id: "run-main", kind: "dataFlow", config: { pipeline: "main" } }],
  );
  return { document, publish: publish(options) };
}

function cdcMirror(options: TemplateOptions): BuiltTemplate {
  const sourceSchema = schema([
    field(options.keyColumn, "int64", false, 1),
    field("name", "utf8", true),
    field("updated_at", "int64", true),
  ]);
  const providerComponent = `${options.cdcProvider}.cdc.source`;
  const sourceConfig: Configuration = {
    connection: options.sourceConnection.id,
    table: options.sourceTable,
    schema: sourceSchema,
    mode: "snapshotAndStream",
    microBatchRows: 512,
    schemaPolicy: { policy: "compatible" },
  };
  if (options.cdcProvider === "postgresql") {
    sourceConfig.slot = options.cdcSlot;
    sourceConfig.publication = options.cdcPublication;
    if (options.appendOnly) sourceConfig.appendOnly = true;
  } else {
    sourceConfig.serverId = options.cdcServerId;
  }
  const source = node("changes", providerComponent, sourceConfig);
  const target = node("mirror", "sql.destination", {
    connection: options.targetConnection.id,
    table: options.targetTable,
    mode: "changeset",
    keys: [0],
    beforeField: "before",
    delivery: "receipt",
    provisionReceipts: true,
  });
  const document = base(
    options,
    [connection(options.sourceConnection), connection(options.targetConnection)],
    [{ id: "main", components: [source, target], paths: [edge("changes", "mirror")] }],
    [{ id: "run-main", kind: "dataFlow", config: { pipeline: "main" } }],
  );
  return { document, publish: publish(options) };
}

function metrics(options: TemplateOptions): BuiltTemplate {
  const orderSchema = schema([
    field("order_id", "int64", false, 1),
    field("shop_id", "int64"),
    field("order_time", "int64"),
  ]);
  const paymentSchema = schema([
    field("event_id", "utf8"),
    field("order_id", "int64"),
    field("event_time", "int64"),
    field("net_amount", "int64"),
    field("kind", "utf8"),
  ]);
  const shopSchema = schema([
    field("shop_id", "int64"),
    field("shop_name", "utf8"),
  ]);
  const order = node("orders", "postgresql.cdc.source", {
    connection: options.sourceConnection.id,
    table: options.sourceTable,
    slot: options.cdcSlot,
    publication: options.cdcPublication,
    mode: "snapshotAndStream",
    appendOnly: true,
    schema: orderSchema,
  });
  const orderTime = node("orderTime", "eventTime", {
    field: "order_time",
    lagMicros: 86_400_000_000,
  });
  const payments = node("payments", "mq.source", {
    connection: options.paymentsConnection?.id ?? "payments",
    targetType: "topic",
    address: options.paymentsTopic,
    format: "json",
    group: `${idPart(options.packageId, "metrics")}-payments`,
    initialPosition: "earliest",
    partition: 0,
    prefetch: 16,
    maxMessageBytes: 65536,
    maxInFlightBytes: 262144,
    schema: paymentSchema,
  });
  const paymentTime = node("paymentTime", "eventTime", {
    field: "event_time",
    lagMicros: 86_400_000_000,
  });
  const deduplicate = node("deduplicate", "eventDeduplicate", {
    keys: ["event_id"],
    timeField: "event_time",
    ttlMicros: 86_400_000_000,
    stateBytes: 65536,
  });
  const deduplicatedTime = node("deduplicatedTime", "eventTime", {
    field: "event_time",
    lagMicros: 86_400_000_000,
  });
  const join = node("join", "eventJoin", {
    leftKeys: ["order_id"],
    rightKeys: ["order_id"],
    leftTimeField: "order_time",
    rightTimeField: "event_time",
    withinMicros: 1000,
    stateBytes: 65536,
  });
  const shops = node("shops", "cacheLookup", {
    cache: "shops",
    keys: [1],
    errorPolicy: "fail",
  });
  const lookupTime = node("lookupTime", "eventTime", {
    field: "event_time",
    lagMicros: 86_400_000_000,
  });
  const window = node("window", "eventWindow", {
    mode: "tumbling",
    timeField: "event_time",
    sizeMicros: 1000,
    allowedLatenessMicros: 0,
    latePolicy: "fail",
    emit: "final",
    plan: {
      version: PROTOCOL_VERSION,
      group: ["shop_id"],
      aggregates: [{ function: "sum", column: "net_amount", name: "net_total" }],
    },
  });
  const fanout = node("fanout", "multicast", { outputs: ["SQL 指标", "Iceberg 指标"] });
  const sqlMetric = node("sqlMetric", "sql.destination", {
    connection: options.targetConnection.id,
    table: options.targetTable,
    mode: "upsert",
    keys: [0, 1, 2],
    delivery: "receipt",
    provisionReceipts: true,
  });
  const icebergMetric = node("icebergMetric", "iceberg.destination", {
    connection: options.icebergCatalog.id,
    storageConnection: options.icebergStorage.id,
    namespace: options.icebergNamespace,
    table: options.icebergTable,
    mode: "upsert",
    keys: [0, 1, 2],
  });
  const shopSource = node("shopSource", "sql.source", {
    connection: options.shopsConnection?.id ?? "shops",
    table: options.shopsTable,
    schema: shopSchema,
    sql: `SELECT shop_id, shop_name FROM ${options.shopsTable}`,
    batchSize: 16,
    schemaPolicy: {
      policy: "explicitMapping",
      mapping: [
        { expected: "shop_id", actual: "shop_id" },
        { expected: "shop_name", actual: "shop_name" },
      ],
    },
  });
  const shopCache = node("shopCache", "cacheBuild", {
    cache: "shops",
    mapping: [0, 1],
  });
  const build: PackageDocument["dataFlows"][number] = {
    id: "buildShops",
    components: [shopSource, shopCache],
    paths: [edge("shopSource", "shopCache")],
  };
  const metric: PackageDocument["dataFlows"][number] = {
    id: "metric",
    components: [
      order,
      orderTime,
      payments,
      paymentTime,
      deduplicate,
      deduplicatedTime,
      join,
      shops,
      lookupTime,
      window,
      fanout,
      sqlMetric,
      icebergMetric,
    ],
    paths: [
      edge("orders", "orderTime"),
      edge("payments", "paymentTime"),
      edge("paymentTime", "deduplicate"),
      edge("deduplicate", "deduplicatedTime"),
      edge("orderTime", "join", 0, 0),
      edge("deduplicatedTime", "join", 0, 1),
      edge("join", "shops"),
      edge("shops", "lookupTime"),
      edge("lookupTime", "window"),
      edge("window", "fanout"),
      edge("fanout", "sqlMetric", 0),
      edge("fanout", "icebergMetric", 1),
    ],
  };
  const document = base(
    options,
    [
      connection(options.sourceConnection),
      connection(options.targetConnection),
      connection(options.paymentsConnection ?? {
        id: "payments",
        driver: "kafka",
        resource: "kafka://payments",
      }),
      connection(options.shopsConnection ?? {
        id: "shops",
        driver: "mysql",
        resource: "mysql://shops",
      }),
      connection(options.icebergCatalog),
      connection(options.icebergStorage),
    ],
    [build, metric],
    [
      { id: "buildShops", kind: "dataFlow", config: { pipeline: "buildShops" } },
      { id: "metric", kind: "dataFlow", config: { pipeline: "metric" } },
    ],
    [{ from: "buildShops", to: "metric", on: "success" }],
  );
  document.caches = [
    {
      id: "shops",
      sourceConnection: options.shopsConnection?.id ?? "shops",
      schema: shopSchema,
      keys: [0],
      scope: "execution",
    },
  ];
  return { document, publish: publish(options) };
}

export function buildJobTemplate(options: TemplateOptions): BuiltTemplate {
  if (options.kind === "batch-incremental") return batchIncremental(options);
  if (options.kind === "cdc-mirror") return cdcMirror(options);
  return metrics(options);
}

export const templateNames: Record<JobTemplateKind, string> = {
  "batch-incremental": "批量增量同步",
  "cdc-mirror": "持续 CDC 镜像",
  "orders-payments-shops-iceberg": "订单支付门店实时指标",
};
