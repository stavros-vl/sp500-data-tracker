# Terraform

[Terraform](https://www.terraform.io/) is an IaC (Infrastructure as Code) software. 
We are going to use it here to create the following GCP resources:
- A GCP bucket, that will be used as our data lake
- A dataset that will be the place where we store our tables.

## Main terraform commands

Install

1. `terraform init`:
    - Initializes & configures the backend, installs plugins/providers, & checks out an existing configuration from a version control
2. `terraform plan`:
    - Matches/previews local changes against a remote state, and proposes an Execution Plan.
3. `terraform apply`:
    - Asks for approval to the proposed plan, and applies changes to cloud
4. `terraform destroy`:
    - Removes your stack from the Cloud

## Execution

Make sure you have installed Terraform first. Link: https://developer.hashicorp.com/terraform/tutorials/aws-get-started/install-cli
```
# Refresh service-account's auth-token for this session
gcloud auth application-default login```

# Initialize state file (.tfstate)
terraform init

# Check changes to new infra plan
terraform plan -var="project=<your-gcp-project-id>"
```

```
# Create new infra
terraform apply -var="project=<your-gcp-project-id>"
```
```
# Delete infra after your work, to avoid costs on any running services
terraform destroy
```

## References
https://learn.hashicorp.com/collections/terraform/gcp-get-started

