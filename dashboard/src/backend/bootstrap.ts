import PocketBase from "pocketbase";

const MEASUREMENTS_COLLECTION = "measurements";

export interface BootstrapResult {
  collections: {
    measurements: {
      name: string;
      created: boolean;
      indexed: boolean;
    };
  };
  security: {
    publicRead: boolean;
    authenticatedWrite: boolean;
  };
}

/**
 * Bootstrap PocketBase collections and security rules
 * This runs on both Docker and bare-metal deployments
 */
export async function bootstrapPocketBase(
  pb: PocketBase,
  adminEmail: string,
  adminPassword: string
): Promise<BootstrapResult> {
  try {
    // Test authentication
    console.log("Testing PocketBase admin authentication...");
    const authData = await pb.admins.authWithPassword(adminEmail, adminPassword);

    if (!authData.record || !authData.token) {
      throw new Error("Failed to authenticate with PocketBase admin credentials");
    }

    console.log("✓ PocketBase admin authentication successful");

    // Create or verify measurements collection
    console.log("Checking measurements collection...");
    let measurementsCollection = await getOrCreateCollection(
      pb,
      MEASUREMENTS_COLLECTION
    );

    // Ensure all required fields exist
    await ensureFields(pb, measurementsCollection);

    // Create indexes
    await ensureIndexes(pb, MEASUREMENTS_COLLECTION);

    // Apply security rules
    await applySecurityRules(pb, MEASUREMENTS_COLLECTION);

    console.log("✓ PocketBase bootstrap completed successfully");

    return {
      collections: {
        measurements: {
          name: MEASUREMENTS_COLLECTION,
          created: true,
          indexed: true,
        },
      },
      security: {
        publicRead: true,
        authenticatedWrite: true,
      },
    };
  } catch (error) {
    console.error(
      "PocketBase bootstrap failed:",
      error instanceof Error ? error.message : error
    );
    throw error;
  }
}

async function getOrCreateCollection(
  pb: PocketBase,
  name: string
): Promise<any> {
  try {
    // Try to get existing collection
    return await pb.collections.getOne(name);
  } catch (error: any) {
    // If collection doesn't exist, create it
    if (error?.status === 404) {
      console.log(`Creating collection: ${name}`);
      return await pb.collections.create({
        name,
        type: "base",
        fields: getRequiredFields(),
      });
    }
    throw error;
  }
}

async function ensureFields(pb: PocketBase, collection: any): Promise<void> {
  const requiredFields = getRequiredFields();
  const existingFields = collection.fields || [];
  const existingFieldNames = new Set(existingFields.map((f: any) => f.name));

  const missingFields = requiredFields.filter(
    (field) => !existingFieldNames.has(field.name)
  );

  if (missingFields.length === 0) {
    return;
  }

  for (const field of missingFields) {
    console.log(`Adding field: ${field.name}`);
  }

  await pb.collections.update(collection.id, {
    fields: [...existingFields, ...missingFields],
  });
}

function getRequiredFields(): Array<{ name: string; type: string; [option: string]: unknown }> {
  return [
    { name: "source_id", type: "text", required: true },
    { name: "ts", type: "date", required: true },
    { name: "gps_lat", type: "number", required: false },
    { name: "gps_lng", type: "number", required: false },
    { name: "sensor_heading_deg", type: "number", required: false },
    { name: "sensor_speed_mps", type: "number", required: false },
    { name: "true_wind_dir_deg", type: "number", required: false },
    { name: "true_wind_speed_mps", type: "number", required: false },
    { name: "apparent_wind_dir_deg", type: "number", required: false },
    { name: "apparent_wind_speed_mps", type: "number", required: false },
    // When the record reached PocketBase. The SAP uplink uses it as its cursor,
    // so measurements that arrive late (with an old ts) are still forwarded.
    { name: "created_at", type: "autodate", onCreate: true, onUpdate: false },
  ];
}

// Indexes on the measurements collection, by name
const INDEXES: Record<string, (collectionName: string) => string> = {
  // The SAP uplink polls each source for records stored after its cursor
  idx_measurements_source_created_at: (c) =>
    `CREATE INDEX \`idx_measurements_source_created_at\` ON \`${c}\` (\`source_id\`, \`created_at\`)`,
};

async function ensureIndexes(pb: PocketBase, collectionName: string): Promise<void> {
  const collection = await pb.collections.getOne(collectionName);
  const existing: string[] = collection.indexes || [];
  const missing = Object.entries(INDEXES).filter(
    ([name]) => !existing.some((sql) => sql.includes(`\`${name}\``))
  );

  if (missing.length === 0) {
    return;
  }

  for (const [name] of missing) {
    console.log(`Adding index: ${name}`);
  }

  await pb.collections.update(collection.id, {
    indexes: [...existing, ...missing.map(([, sql]) => sql(collectionName))],
  });
}

async function applySecurityRules(pb: PocketBase, collectionName: string): Promise<void> {
  // Apply collection-level rules:
  // - Public read access (empty list/view rules)
  // - Authenticated write access (create/update/delete rules)

  console.log(`Applying security rules to ${collectionName} collection`);

  const collection = await pb.collections.getOne(collectionName);

  try {
    await pb.collections.update(collection.id, {
      // Empty string means public access for list/view in PocketBase.
      listRule: "",
      viewRule: "",
      // Authenticated write
      createRule: "@request.auth.id != ''",
      updateRule: "@request.auth.id != ''",
      deleteRule: "@request.auth.id != ''",
    });
  } catch (error: any) {
    const details = error?.response?.data || error?.response || error;
    console.error("Failed to apply security rules details:", details);
    throw error;
  }

  console.log(`✓ Security rules applied`);
}
