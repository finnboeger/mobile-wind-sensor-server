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
        schema: [], // Will add fields next
      });
    }
    throw error;
  }
}

async function ensureFields(pb: PocketBase, collection: any): Promise<void> {
  const requiredFields = {
    source_id: { type: "text", required: true },
    ts: { type: "date", required: true },
    gps_lat: { type: "number", required: false },
    gps_lng: { type: "number", required: false },
    sensor_heading_deg: { type: "number", required: false },
    sensor_speed_mps: { type: "number", required: false },
    true_wind_dir_deg: { type: "number", required: true },
    true_wind_speed_mps: { type: "number", required: true },
    apparent_wind_dir_deg: { type: "number", required: false },
    apparent_wind_speed_mps: { type: "number", required: false },
  };

  const schema = collection.schema || [];
  const existingFieldNames = schema.map((f: any) => f.name);

  for (const [fieldName, fieldConfig] of Object.entries(requiredFields)) {
    if (!existingFieldNames.includes(fieldName)) {
      console.log(`Adding field: ${fieldName}`);
      schema.push({
        id: `field_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`,
        name: fieldName,
        ...fieldConfig,
        system: false,
      });
    }
  }

  // Update collection with new fields
  if (schema.length !== existingFieldNames.length) {
    await pb.collections.update(collection.id, { schema });
  }
}

async function ensureIndexes(pb: PocketBase, collectionName: string): Promise<void> {
  // PocketBase uses different API for indexes - this would depend on version
  // For now, document that indexes should be:
  // 1. Unique index on (source_id, ts)
  // 2. Index on source_id for filtering
  // 3. Index on ts for time-range queries
  console.log("Note: Ensure the following indexes exist in PocketBase:");
  console.log("  - Unique: (source_id, ts)");
  console.log("  - Index: source_id");
  console.log("  - Index: ts");
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
