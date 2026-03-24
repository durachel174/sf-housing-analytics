// Lambda: GET /neighborhoods
//         GET /neighborhoods/:name
//
// Returns neighborhood summaries and top-risk buildings within a neighborhood.

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
  const db = getPool();
  const name = event.pathParameters?.name
    ? decodeURIComponent(event.pathParameters.name)
    : null;

  try {
    if (name) {
      // Single neighborhood — stats + top 10 riskiest buildings
      const [statsRes, buildingsRes] = await Promise.all([
        db.query("SELECT * FROM neighborhood_stats WHERE neighborhood = $1", [name]),
        db.query(
          `SELECT blklot, address, risk_score, units, year_built, cluster
           FROM buildings
           WHERE neighborhood = $1
           ORDER BY risk_score DESC
           LIMIT 10`,
          [name]
        ),
      ]);

      if (!statsRes.rows[0]) {
        return {
          statusCode: 404,
          headers: HEADERS,
          body: JSON.stringify({ error: "Neighborhood not found" }),
        };
      }

      return {
        statusCode: 200,
        headers: HEADERS,
        body: JSON.stringify({
          neighborhood: statsRes.rows[0],
          topRiskBuildings: buildingsRes.rows,
        }),
      };
    }

    // All neighborhoods — sorted by risk
    const res = await db.query(
      `SELECT neighborhood, total_buildings, open_violations, violation_rate,
              median_risk_score, cluster, top_violation_type
       FROM neighborhood_stats
       ORDER BY median_risk_score DESC`
    );

    return {
      statusCode: 200,
      headers: HEADERS,
      body: JSON.stringify({ neighborhoods: res.rows }),
    };
  } catch (err) {
    console.error("Neighborhoods error:", err);
    return {
      statusCode: 500,
      headers: HEADERS,
      body: JSON.stringify({ error: "Database error" }),
    };
  }
};
