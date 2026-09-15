# ADR 0006: GitHub Actions controls CI/CD

- Status: Proposed
- Date: 2026-09-15

## Context

The framework source of truth is a GitHub repository. Delivery must test the production Spark
code, build and retain an exact wheel, deploy Fabric item definitions, publish that wheel to a
Fabric Environment, resolve workspace-scoped bindings and execute real Fabric UAT.

Fabric Deployment Pipelines are useful for promoting supported workspace items, but they are not
a complete CI system and do not build or test the Python package. Spark Job Definition references
such as the attached Environment and default Lakehouse also require explicit verification when
items move across workspaces.

Introducing an Azure DevOps Project only to drive Fabric deployment would add a second delivery
control plane while GitHub remains the source repository.

## Decision

GitHub Actions is the single top-level CI/CD orchestrator.

- Pull-request workflows run Python unit tests, local Spark/Delta tests, architecture guards and
  package validation.
- The release workflow builds one versioned wheel, calculates its SHA256 and retains it as the
  candidate artifact.
- `fabric-cicd` deploys source-controlled Fabric item definitions.
- Stable Fabric REST APIs upload the exact candidate wheel to the target Fabric Environment,
  publish the Environment, resolve or update workspace-scoped item bindings, invoke Spark jobs and
  retrieve deployment and execution evidence.
- GitHub Environments separate Dev, UAT and Prod credentials and provide required approvals.
- Dev, UAT and Prod receive the same wheel bytes. Promotion changes physical bindings, not code.
- Real Fabric UAT must pass before the production GitHub Environment can be approved.

Fabric Deployment Pipelines are optional. When organizational governance requires them, GitHub
Actions invokes and observes the Deployment Pipeline through its API. The Deployment Pipeline is
then a deployment mechanism, not a second source of release truth or an independent orchestrator.

An Azure DevOps Project is not required while GitHub remains the repository and CI/CD platform.
Adopting Azure Pipelines later requires an explicit superseding ADR and one controlled migration,
not simultaneous GitHub and Azure DevOps release authorities.

## Deployment evidence

Every deployed release must bind at least:

- Git commit SHA;
- wheel filename, package version and SHA256;
- logical configuration hash and environment-binding hash;
- target workspace, Environment, Lakehouse and Spark Job Definition identities;
- Fabric runtime version;
- deployment run and Spark execution identities;
- Fabric UAT result for the exact candidate.

## Configuration ownership

Environment manifests are the deployment input. If Fabric Variable Library is used, automation
materializes or selects its stage-specific values from that input. The same physical setting must
not be maintained manually in both repository YAML and a Variable Library.

Secrets remain outside Git and are supplied through protected GitHub Environments or approved
enterprise secret stores.

## Consequences

- CI and CD history is visible in the same platform as source and pull requests.
- The exact package tested in CI is the package installed in Fabric.
- Fabric portal state is reconciled and verified after deployment instead of being assumed.
- Native Deployment Pipelines remain available for governance without creating a competing release
  authority.
- CD requires idempotent deployment scripts and explicit post-deployment binding verification.

## References

- [Microsoft Fabric CI/CD overview](https://learn.microsoft.com/en-us/fabric/cicd/cicd-overview)
- [Fabric Git integration](https://learn.microsoft.com/en-us/fabric/cicd/git-integration/git-get-started)
- [Fabric Deployment Pipelines](https://learn.microsoft.com/en-us/fabric/cicd/deployment-pipelines/intro-to-deployment-pipelines)
- [Fabric Environment public APIs](https://learn.microsoft.com/en-us/fabric/data-engineering/environment-public-api)
- [Spark Job Definition source control](https://learn.microsoft.com/en-us/fabric/data-engineering/spark-job-definition-source-control)
