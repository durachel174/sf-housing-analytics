// Lambda: GET /building?address=123+Main+St
// or:      GET /building?blklot=01234567
//
// Returns full building profile: risk score, violations history,
// neighborhood comparison, similar buildings nearby.
// This is the core product endpoint.

const { Pool } = require("pg");

// Connection pool — reused across warm Lambda invocations
let pool;
function getPool() {
  if (!pool) {
    pool = new Pool({
      host: process.env.DB_HOST,
      port: parseInt(process.env.DB_PORT || "5432"),
      database: process.env.DB_NAME,
      user: process.env.DB_USER,
      password: process.env.DB_PASSWORD,
      ssl: { rejectUnauthorized: false }, // RDS SSL
      max: 2,        // keep Lambda pool small
      idleTimeoutMillis: 10000,
    });
  }
  return pool;
}

const HEADERS = {
  "Content-Type": "application/json",
  "Access-Control-Allow-Origin": process.env.FRONTEND_ORIGIN || "*",
};

exports.handler = async (event) => {
  const { blklot, address } = event.queryStringParameters || {};

  if (!blklot && !address) {
    return {
      statusCode: 400,
      headers: HEADERS,
      body: JSON.stringify({ error: "blklot or address required" }),
    };
  }

  const db = getPool();

  try {
    // 1. Find the building
    let buildingRow;
    if (blklot) {
      const res = await db.query(
        "SELECT * FROM buildings WHERE blklot = $1",
        [blklot]
      );
      buildingRow = res.rows[0];
    } else {
      // Fuzzy address search using ILIKE
      const res = await db.query(
        "SELECT * FROM buildings WHERE address ILIKE $1 ORDER BY risk_score DESC LIMIT 1",
        [`%${address}%`]
      );
      buildingRow = res.rows[0];
    }

    if (!buildingRow) {
      return {
        statusCode: 404,
        headers: HEADERS,
        body: JSON.stringify({ error: "Building not found" }),
      };
    }

    // 2. Violations for this building (last 5 years)
    const violRes = await db.query(
      `SELECT complaint_type, status, date_filed, date_closed, disposition
       FROM violations
       WHERE blklot = $1
       ORDER BY date_filed DESC
       LIMIT 50`,
      [buildingRow.blklot]
    );

    // 3. Neighborhood context (for comparison)
    const nbhdRes = await db.query(
      "SELECT * FROM neighborhood_stats WHERE neighborhood = $1",
      [buildingRow.neighborhood]
    );
    const nbhd = nbhdRes.rows[0] || {};

    // 4. Similar buildings nearby (same neighborhood, similar unit count)
    const similarRes = await db.query(
      `SELECT blklot, address, risk_score, units, year_built, cluster
       FROM buildings
       WHERE neighborhood = $1
         AND blklot != $2
         AND units BETWEEN $3 AND $4
       ORDER BY risk_score ASC
       LIMIT 5`,
      [
        buildingRow.neighborhood,
        buildingRow.blklot,
        Math.max(1, (buildingRow.units || 1) - 10),
        (buildingRow.units || 1) + 10,
      ]
    );

    // 5. Violation breakdown by type
    const typeRes = await db.query(
      `SELECT complaint_type, COUNT(*) as count,
              SUM(CASE WHEN status = 'open' THEN 1 ELSE 0 END) as open_count
       FROM violations
       WHERE blklot = $1
       GROUP BY complaint_type
       ORDER BY count DESC`,
      [buildingRow.blklot]
    );

    return {
      statusCode: 200,
      headers: HEADERS,
      body: JSON.stringify({
        building: buildingRow,
        violations: violRes.rows,
        violationBreakdown: typeRes.rows,
        neighborhood: nbhd,
        similarBuildings: similarRes.rows,
      }),
    };
  } catch (err) {
    console.error("DB error:", err);
    return {
      statusCode: 500,
      headers: HEADERS,
      body: JSON.stringify({ error: "Database error" }),
    };
  }
};
