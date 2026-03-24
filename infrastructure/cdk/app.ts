import * as cdk from 'aws-cdk-lib';
import { SfHousingStack } from './stack';

const app = new cdk.App();
new SfHousingStack(app, 'SfHousingStack', {
  env: {
    account: process.env.CDK_DEFAULT_ACCOUNT,
    region: process.env.CDK_DEFAULT_REGION,
  },
});
