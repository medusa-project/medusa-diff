# Medusa Diff

A dockerized Ruby app to determine the difference between what is in the Medusa Database and the Medusa S3 bucket.

## Prerequisites

- Ruby (version 3.4.5)
- Bundler

## Setup

Clone the repository and install dependencies:
```shell
git clone git@github.com:medusa-project/medusa-diff.git
cd medusa-diff
bundle install
```

## Deployment
Use the deploy directory scripts:
```shell
./deploy/ecr-push.sh <environment>
```

## Usage
Run the app on fargate:
```shell
require 'aws-sdk-ecs'

client = Aws::ECS::Client.new(
  region: #Region,
)

repos = [#Repositories]
resp = client.run_task({
  cluster: "medusa-diff",
  count: 1,
  enable_ecs_managed_tags: true,
  launch_type: "FARGATE",
  network_configuration: {
    awsvpc_configuration: {
      subnets: [#Subnets],
      security_groups: [#Security Groups],
      assign_public_ip: "DISABLED",
    },
  },
  overrides: {
    container_overrides: [
      {
        name: "medusa-diff-prod-task",
        command: ["bundle","exec","ruby","-r","./lib/medusa_diff.rb","-e","MedusaDiff.run #{repos}"],
      },
    ],
  },
  platform_version: "1.4.0",
  propagate_tags: "TASK_DEFINITION",
  task_definition: "medusa-diff-prod-td:2",
})
```

## Testing
This will run the tests in a docker container to make use of Mockdusa:
```shell
docker-compose up --build
```
