# AWS Datalake Boilerplate

This repository is a tool that assists in creating an AWS datalake with some basic features using infrastructure as code. These features include:

- Creation of buckets in a mesh structure by default, where each domain has specific data.
- Creation of 3 standard layers for data movement: raw, trusted, and service.
- Default SNS Topic to notify pipeline errors.
- Quick deployment of pipelines to move data between 2 layers within a domain.
- Flexibility to add any other standalone stacks on top of the default datalake resources.

Since its a template with essential features to build, test and deploy a datalake fast, there are many resources that you can adjust to your specific deployment.

## Contribute to this project

As this template was designed with certain specific features in mind, it may not cover all possible scenarios. However, if you have made an interesting modification and would like to include it in the template, we encourage you to contribute to this project. Your contribution would greatly enrich and expand the project.

We also appreciate any improvements in documentation and code quality. Your efforts in these areas are highly valued!

## Project Dependencies

To run this project, the following dependencies should be installed:

- Python 3.9+
- Docker Engine 23+
- AWS CLI
- dev-requirements *

\* Python libraries used by the project, which can be extended according to the needs of each implementation.


## Project Structure
```
📦root
├─📦builder               :: Interprets pipelines and compiles resources
├─📦ci                    :: Examples of CI pipelines for multiple platforms
├─📦docker                :: Dockerfiles for Glue Jobs and Lambda functions
├─📦pipelines             :: Assets and settings for each pipeline
├─📦shared                :: Modules to be used by various Glue and Lambda codes
├─📦standalone            :: Additional CDK stacks, specific to each project
├─🗿app.py                :: Main script for datalake compilation
├─⚙️cdk.json              :: AWS CDK Settings
├─⚙️pyproject.toml        :: Additional project tools settings
├─📜dev-requirements.txt  :: Requirements for development environment
└─📜requirements.txt      :: Requirements for project build
```

# How it works

There are three main components involved in building the datalake using this repository:

- Shared datalake resources used by other stacks.
- Specific resources for a data pipeline.
- Additional project-specific resources that vary in each implementation.

When the command `cdk deploy` runs, it compiles all the dependencies from each pipeline using docker and set-up resources according to each pipeline `config.yml` file.

Also, additional stacks that are attached to the `app.py` file are synth and deployed together with the other resources.

Make sure that you have set some variables in your environment in order to deploy correctly:
 - AWS_ACCESS_KEY_ID
 - AWS_SECRET_ACCESS_KEY
 - AWS_DEFAULT_REGION
 - AWS_ACCOUNT_ID
 - ENVIRONMENT_STAGE

## Datalake shared resources

These resources can be configured directly in the `app.py` file through the `DatalakeBuilder` instance, which include:

  - **lake_name**: Datalake name, which will be displayed in SNS messages and in the names of resources in AWS.
  - **env**: Environment stage of the deployment, it can be 'dev', 'stg' or 'prd'.
  - **lake_domains**: The list of domains in the datalake, each domain contains a bucket for each lake layer.
  - **enable_vpc**: If true, a VPC will be used across all native resources and you can attach new resources by accessing the .vpc property of the DataLakeBuilder.
  - **sns_subscriptions**: list of subscriptions to SNS Topic.
  - **tags**: List of tags shared accross all native resources.


```python
DatalakeBuilder(
    scope=app,
    lake_name="Example Lake",
    region=environ["AWS_DEFAULT_REGION"],
    account_id=environ["AWS_ACCOUNT_ID"],
    env=environ["ENVIRONMENT_STAGE"],
    lake_domains=["example"],
    enable_vpc=False,
    sns_subscriptions=[
        {"protocol": "email", "endpoint": "example@example.com"}
    ],
    pipelines_path=path.join(root, "pipelines"),
    tags={"example1": "value1", "example2": "value2"},
)
```

### List of resources

In the native datalake package, two stacks are generated, which includes:

```
📦lake-storage-stack
└─ AWS::S3::Bucket

📦lake-shared-stack
├─ AWS::Glue::Database
├─ AWS::Glue::Crawler
├─ AWS::IAM::Role
├─ AWS::IAM::Policy
├─ AWS::SNS::Topic
├─ AWS::SNS::Subscription
├─ [optional] AWS::EC2::VPC
├─ [optional] AWS::EC2::Route
├─ [optional] AWS::EC2::Subnet
├─ [optional] AWS::EC2::RouteTable
├─ [optional] AWS::EC2::SubnetRouteTableAssociation
├─ [optional] AWS::EC2::InternetGateway
├─ [optional] AWS::EC2::VPCGatewayAttachment
├─ [optional] AWS::EC2::SecurityGroup
└─ [optional] AWS::EC2::VPCEndpoint
```

## Pipeline resources

These resources are generated according to each pipeline in the `pipeline` folder, in the `config.yml` file. For now, built-in pipelines can handle just a little customization:

 - It accepts lambda and glue tasks only
 - It accepts state machine native choice only
 - It accepts events from S3 and Eventrule for now
 - Glue tasks can be `pythonshell` or `glueetl`
 - The output of Glue jobs are it's own input by default
 - The input of each task is exactly the output of the previous one
 - Each task directs to a catch error function if it fails
 - The catch error function notifies the datalake SNS Topic

In the future releases, some new built-in features can be added to the pipeline structure.

The structure of the config file is the following:

 - **name**: Pipeline name, used by its resources.
 - **domain**: Which domain the pipeline is deployed, must be one of the domains related in the datalake config.
 - **layers**: Origin and target layers of the pipeline. 
 - **triggers**: List of events that initialize the pipeline, it can be S3 trigger or a Event Rule.
 - **tags**: Tags that are attached to the resources.
 - **contract**: The contract used by the pipeline, must be the same output of the trigger script, its used as reference for glue jobs and for documentation.
 - **steps**: List of steps that runs in the pipeline, it can be Lambda, Glue or choice, more details about options of these steps are related below.

Example of a config file:

```
name: pipeline_example
domain: example
layers:
  origin: raw
  target: trusted

triggers:
  - s3:
      prefix: example/
      suffix: .json
  - event_rule:
      source: ["example_source"]
      detail_type: ["example_detail"]

tags:
  key1: value1

contract:
  key1: str
  key2: str

steps:
  ExampleLambda:
    type: lambda
    properties:
      module: example_lambda_folder
      next_step: ExampleChoice
      timeout_seconds: 60
      memory_size: 128
  ExampleChoice:
    type: choice
    properties:
      choices:
        - variable: example
          equals: value1
          next_step: ExampleGlue
  ExampleGlue:
    type: glue
    properties:
      module: example_glue_folder
      glue_version: pythonshell
      timeout_minutes: 30
      max_concurrent_runs: 25
```

For more details about which properties you can setup in a lambda or glue resource, look at the respective resource model class (`./builder/model/resource/`), in the `.from_pydict` static method.

in addition to resource properties, there are these pipeline properties:
 - **module**: The name of the folder within the steps folder where is the script.
 - **next_step**: The name of the key that represents the next step after the current one.

The structure of a pipeline directory must be the following:

```
📦root
├─📦catch                 :: Script of the lambda that catch error from the pipeline
├─📦steps                 :: Folder where the code of each step are, organized in modules
| └─📦...
├─📦trigger               :: Script that receives an event an trigger the pipeline
└─📜config.yml            :: Pipeline's config file.
```

Each module, trigger and catch folders follows the same structure:

```
📦module
├─📦src
| ├─📦tests
| ├─...
| └─🗿index.py
└─📜requirements.json
```

### Setup dependencies of tasks

The dependencies of each module is controlled by the `requirements.json` file, where you can specify tree things:
 - **packages**: Similar to the traditional requirements.txt, use this field to declare an array of libraries that the module needs to run.
 - **extra_jars**: In this field, you can declare additional jars that a Glue job need to run. Different from packages, in this case you need to specify the direct download link of the dependency.
 - **shared_modules**: Since the shared library isn't compiled as a whole single package, you can specify which shared module you're using in that specific script. Only the specified modules will be compiled together with the another dependencies in the build process.

 ```
 {
    "packages": [],
    "extra_jars": [],
    "shared_modules": []
}
 ```

### List of resources

For each pipeline package package, a single stack is generated, which includes:

```
📦lake-shared-stack
├─ Custom::S3BucketNotifications
├─ AWS::IAM::Role
├─ AWS::IAM::Policy
├─ AWS::Lambda::Function
├─ AWS::Glue::Job
├─ AWS::Events::Rule
├─ AWS::StepFunctions::StateMachine
├─ [optional] AWS::EC2::VPC
├─ [optional] AWS::EC2::Subnet
├─ [optional] AWS::EC2::RouteTable
├─ [optional] AWS::EC2::SubnetRouteTableAssociation
├─ [optional] AWS::EC2::Route
├─ [optional] AWS::EC2::InternetGateway
├─ [optional] AWS::EC2::VPCGatewayAttachment
└─ [optional] AWS::EC2::SecurityGroup
```

## Standalone resources

Every project has its unique aspects, so relying solely on buckets, databases, crawlers, and pipelines isn't sufficient to meet all implementation requirements. Hence, it becomes necessary to incorporate additional resources like standalone stacks.

These stacks can be directly integrated into the `app.py` and leverage shared resources from the datalake, such as bucket names, VPC, and SNS topics. To ensure repository organization, it is advisable to store all supplementary stacks in the `standalone` folder.
