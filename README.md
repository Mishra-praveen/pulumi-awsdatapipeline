# pulumi-awsdatapipeline


This project is used to setup aws datapipeline using Pulumi.

Create a new stack, which is an isolated deployment target for this example:

$ pulumi stack init test

Set your desired AWS region:

$ pulumi config set aws:region us-east-1 # any valid AWS region will work
$ pulumi config set source_s3_bucket
$ pulumi config set destination_table_name

Deploy everything with a single pulumi up command. This will show you a preview of changes first, which includes all of the required AWS resources (clusters, services, and the like). Don't worry if it's more than you expected -- this is one of the benefits of Pulumi, it configures everything so that so you don't need to!

$ pulumi up