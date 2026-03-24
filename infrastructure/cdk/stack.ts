// AWS CDK Stack v2 — SF Housing Intelligence
// Changes from v1:
//   - DynamoDB → RDS Postgres (t3.micro, free tier eligible)
//   - No Secrets Manager for Anthropic (AI removed from v1)
//   - Lambdas updated to query Postgres via pg pool
//   - VPC for RDS + Lambda (required for RDS access)
//   - EventBridge schedule for ETL (manual trigger for v1)

import * as cdk from "aws-cdk-lib";
import { Construct } from "constructs";
import * as s3 from "aws-cdk-lib/aws-s3";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as apigateway from "aws-cdk-lib/aws-apigateway";
import * as rds from "aws-cdk-lib/aws-rds";
import * as ec2 from "aws-cdk-lib/aws-ec2";
import * as secretsmanager from "aws-cdk-lib/aws-secretsmanager";
import * as amplify from "@aws-cdk/aws-amplify-alpha";
import * as events from "aws-cdk-lib/aws-events";
import * as targets from "aws-cdk-lib/aws-events-targets";
import * as path from "path";

export class SfHousingStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    // -----------------------------------------------------------------------
    // VPC — required so Lambdas can reach RDS privately
    // -----------------------------------------------------------------------
    const vpc = new ec2.Vpc(this, "Vpc", {
      maxAzs: 2,
      natGateways: 1, // Lambdas need outbound internet for SF Open Data calls
      subnetConfiguration: [
        { name: "public", subnetType: ec2.SubnetType.PUBLIC, cidrMask: 24 },
        { name: "private", subnetType: ec2.SubnetType.PRIVATE_WITH_EGRESS, cidrMask: 24 },
        { name: "isolated", subnetType: ec2.SubnetType.PRIVATE_ISOLATED, cidrMask: 28 },
      ],
    });

    // -----------------------------------------------------------------------
    // Security groups
    // -----------------------------------------------------------------------
    const lambdaSg = new ec2.SecurityGroup(this, "LambdaSg", {
      vpc,
      description: "SF Housing Lambda functions",
      allowAllOutbound: true,
    });

    const rdsSg = new ec2.SecurityGroup(this, "RdsSg", {
      vpc,
      description: "SF Housing RDS Postgres",
      allowAllOutbound: false,
    });
    // Allow Lambdas to connect to Postgres on port 5432
    rdsSg.addIngressRule(lambdaSg, ec2.Port.tcp(5432), "Lambda to Postgres");

    // -----------------------------------------------------------------------
    // S3 — raw data archive
    // -----------------------------------------------------------------------
    const dataBucket = new s3.Bucket(this, "DataBucket", {
      bucketName: `sf-housing-data-v2-${this.account}`,
      versioned: true,
      removalPolicy: cdk.RemovalPolicy.RETAIN,
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      encryption: s3.BucketEncryption.S3_MANAGED,
      lifecycleRules: [
        {
          id: "archive-raw-90d",
          prefix: "raw/",
          transitions: [
            { storageClass: s3.StorageClass.GLACIER, transitionAfter: cdk.Duration.days(90) },
          ],
        },
      ],
    });

    // -----------------------------------------------------------------------
    // RDS Postgres — t3.micro (free tier eligible for 12 months)
    // -----------------------------------------------------------------------
    const dbSecret = new secretsmanager.Secret(this, "DbSecret", {
      secretName: "sf-housing/db-credentials",
      generateSecretString: {
        secretStringTemplate: JSON.stringify({ username: "sfhousing" }),
        generateStringKey: "password",
        excludePunctuation: true,
        passwordLength: 32,
      },
    });

    const db = new rds.DatabaseInstance(this, "Postgres", {
      engine: rds.DatabaseInstanceEngine.postgres({
        version: rds.PostgresEngineVersion.VER_16,
      }),
      instanceType: ec2.InstanceType.of(ec2.InstanceClass.T3, ec2.InstanceSize.MICRO),
      vpc,
      vpcSubnets: { subnetType: ec2.SubnetType.PUBLIC },
      securityGroups: [rdsSg],
      credentials: rds.Credentials.fromSecret(dbSecret),
      databaseName: "sfhousing",
      backupRetention: cdk.Duration.days(0),
      deletionProtection: true,
      storageEncrypted: true,
      publiclyAccessible: true,
      removalPolicy: cdk.RemovalPolicy.RETAIN,
      // Enable PostGIS via parameter group
      parameterGroup: new rds.ParameterGroup(this, "PgParams", {
        engine: rds.DatabaseInstanceEngine.postgres({ version: rds.PostgresEngineVersion.VER_16 }),
        parameters: { "shared_preload_libraries": "pg_stat_statements" },
      }),
    });

    // -----------------------------------------------------------------------
    // Shared Lambda config
    // -----------------------------------------------------------------------
    const dbSecretArn = dbSecret.secretArn;

    // DB env vars come from Secrets Manager at deploy time via CDK
    // At runtime, Lambdas read from env (CDK injects from secret)
    const lambdaEnv = {
      DB_HOST: db.instanceEndpoint.hostname,
      DB_PORT: "5432",
      DB_NAME: "sfhousing",
      DB_USER: "sfhousing",
      // DB_PASSWORD injected from secret below
      FRONTEND_ORIGIN: "https://main.YOUR_AMPLIFY_ID.amplifyapp.com",
    };

    const lambdaDefaults = {
      runtime: lambda.Runtime.NODEJS_20_X,
      timeout: cdk.Duration.seconds(15),
      memorySize: 256,
    };

    function makeLambda(scope: Construct, id: string, dir: string, description: string) {
      const fn = new lambda.Function(scope, id, {
        ...lambdaDefaults,
        runtime: lambda.Runtime.NODEJS_20_X,
        functionName: `sf-housing-${dir}`,
        code: lambda.Code.fromAsset(path.join(__dirname, `../../backend/lambdas/${dir}`)),
        handler: "index.handler",
        description,
        environment: {
          ...lambdaEnv,
          DB_SECRET_ARN: dbSecretArn,
        },
      });
      // Allow reading DB password from secret
      dbSecret.grantRead(fn);
      return fn;
    }

    // -----------------------------------------------------------------------
    // Lambda functions
    // -----------------------------------------------------------------------
    const buildingFn = makeLambda(this, "BuildingFn", "building",
      "GET /building — full building profile + violations");
    const searchFn = makeLambda(this, "SearchFn", "search",
      "GET /search — address autocomplete + neighborhood browse");
    const neighborhoodFn = makeLambda(this, "NeighborhoodFn", "neighborhood",
      "GET /neighborhoods — all or single neighborhood stats");

    // ETL runs locally for v1 — placeholder Lambda to satisfy EventBridge target
    const etlFn = new lambda.Function(this, "EtlFn", {
      functionName: "sf-housing-etl",
      runtime: lambda.Runtime.PYTHON_3_12,
      timeout: cdk.Duration.minutes(10),
      memorySize: 1024,
      vpc,
      vpcSubnets: { subnetType: ec2.SubnetType.PRIVATE_WITH_EGRESS },
      securityGroups: [lambdaSg],
      code: lambda.Code.fromInline(
        "def lambda_handler(event, context):\n    print('ETL placeholder')\n"
      ),
      handler: "index.lambda_handler",
      environment: {
        ...lambdaEnv,
        S3_BUCKET: dataBucket.bucketName,
        AWS_REGION_NAME: this.region,
      },
      description: "Weekly ETL placeholder — run etl.py locally for v1",
    });
    dbSecret.grantRead(etlFn);
    dataBucket.grantReadWrite(etlFn);

    // EventBridge: every Sunday 2am UTC
    new events.Rule(this, "EtlSchedule", {
      schedule: events.Schedule.cron({ minute: "0", hour: "2", weekDay: "SUN" }),
      targets: [new targets.LambdaFunction(etlFn)],
      description: "Weekly SF Housing ETL trigger",
    });

    // -----------------------------------------------------------------------
    // API Gateway
    // -----------------------------------------------------------------------
    const api = new apigateway.RestApi(this, "Api", {
      restApiName: "sf-housing-api",
      description: "SF Housing Intelligence API",
      defaultCorsPreflightOptions: {
        allowOrigins: apigateway.Cors.ALL_ORIGINS,
        allowMethods: ["GET", "OPTIONS"],
      },
      deployOptions: {
        stageName: "prod",
        throttlingRateLimit: 100,
        throttlingBurstLimit: 200,
      },
    });

    // GET /building
    api.root
      .addResource("building")
      .addMethod("GET", new apigateway.LambdaIntegration(buildingFn));

    // GET /search
    api.root
      .addResource("search")
      .addMethod("GET", new apigateway.LambdaIntegration(searchFn));

    // GET /neighborhoods
    // GET /neighborhoods/{name}
    const nbhds = api.root.addResource("neighborhoods");
    nbhds.addMethod("GET", new apigateway.LambdaIntegration(neighborhoodFn));
    nbhds
      .addResource("{name}")
      .addMethod("GET", new apigateway.LambdaIntegration(neighborhoodFn));

    // -----------------------------------------------------------------------
    // Amplify — frontend hosting
    // -----------------------------------------------------------------------
    const amplifyApp = new amplify.App(this, "Frontend", {
      appName: "sf-housing-analytics",
      sourceCodeProvider: new amplify.GitHubSourceCodeProvider({
        owner: "durachel174",        // ← update
        repository: "sf-housing-analytics",
        oauthToken: cdk.SecretValue.secretsManager("github-token"),
      }),
      buildSpec: cdk.aws_codebuild.BuildSpec.fromObjectToYaml({
        version: "1.0",
        frontend: {
          phases: {
            preBuild: { commands: ["npm ci"] },
            build: { commands: ["npm run build"] },
          },
          artifacts: { baseDirectory: "dist", files: ["**/*"] },
          cache: { paths: ["node_modules/**/*"] },
        },
      }),
      environmentVariables: {
        VITE_API_BASE_URL: api.url,
      },
    });
    amplifyApp.addBranch("main", { autoBuild: true, stage: "PRODUCTION" });

    // -----------------------------------------------------------------------
    // Outputs
    // -----------------------------------------------------------------------
    new cdk.CfnOutput(this, "ApiUrl", { value: api.url, description: "API Gateway URL" });
    new cdk.CfnOutput(this, "DbEndpoint", { value: db.instanceEndpoint.hostname, description: "RDS endpoint (for local ETL runs)" });
    new cdk.CfnOutput(this, "DataBucketName", { value: dataBucket.bucketName });
    new cdk.CfnOutput(this, "AmplifyUrl", {
      value: `https://main.${amplifyApp.appId}.amplifyapp.com`,
    });
  }
}