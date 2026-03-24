// Lambda: GET /search?q=Mission+St&type=address|neighborhood
//
// Powers the address search bar and neighborhood browser.
// Returns lightweight results — no full violation history.

const { Pool } = require("pg");

let pool;
function getPool() {
  if (!pool) {
    pool = new Pool({
      host: process.env.DB_HOST,
      port: parseInt(process.env.DB_PORT || "5432"),
      database: process.env.DB_NAME,
      user: process.env.DB_USER,
      password: process.env.DB_PASSWORD,
      ssl: { rejectUnauthorized: false },
      max: 2,
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
  const { q, type = "address", limit = "10" } = event.queryStringParameters || {};

  if (!q || q.trim().length < 2) {
    return {
      statusCode: 400,
      headers: HEADERS,
      body: JSON.stringify({ error: "q must be at least 2 characters" }),
    };
  }

  const db = getPool();
  const lim = Math.min(parseInt(limit), 20);

  try {
    if (type === "neighborhood") {
      // Full neighborhood list with stats
      const res = await db.query(
        `SELECT neighborhood, total_buildings, open_violations,
                violation_rate, median_risk_score, cluster, top_violation_type
         FROM neighborhood_stats
         WHERE neighborhood ILIKE $1
         ORDER BY median_risk_score DESC
         LIMIT $2`,
        [`%${q}%`, lim]
      );
      return {
        statusCode: 200,
        headers: HEADERS,
        body: JSON.stringify({ type: "neighborhood", results: res.rows }),
      };
    }

    // Address search — return just enough for autocomplete
    const res = await db.query(
      `SELECT blklot, address, neighborhood, risk_score, units, cluster
       FROM buildings
       WHERE address ILIKE $1
       ORDER BY risk_score DESC
       LIMIT $2`,
      [`%${q}%`, lim]
    );

    return {
      statusCode: 200,
      headers: HEADERS,
      body: JSON.stringify({ type: "address", results: res.rows }),
    };
  } catch (err) {
    console.error("Search error:", err);
    return {
      statusCode: 500,
      headers: HEADERS,
      body: JSON.stringify({ error: "Search failed" }),
    };
  }
};
