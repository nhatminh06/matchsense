# Security Policy

MatchSense is a personal/portfolio project. There is no SLA on response
times, but security reports are taken seriously.

## Reporting a vulnerability

**Do not open a public GitHub issue for a suspected vulnerability.**

Instead, use [GitHub's private vulnerability reporting](https://github.com/nhatminh06/matchsense/security/advisories/new)
for this repository (Security tab → Report a vulnerability). If that's not
available, contact the repository owner directly via their GitHub profile.

Please include:

- A description of the vulnerability and its potential impact
- Steps to reproduce (a minimal repro is very helpful)
- Any relevant logs — redact tokens, credentials, or personal data first

## Scope

This covers the application code in this repository (`services/`, GitOps
manifests, CI workflows) and how it's configured. It does not cover
third-party dependencies themselves — please report those upstream — but
do let us know if MatchSense is using a dependency with a known
vulnerability so it can be updated.

## What's already in place

- Automated secret scanning (Gitleaks) on every push and PR
- Container image scanning (Trivy), blocking on CRITICAL severity
- Software Bill of Materials (SBOM) generated per image
- Image signing (Cosign, keyless/OIDC) and Kyverno admission policies that
  verify signatures, disallow privileged containers, disallow `:latest`
  tags, and require resource limits before a workload is admitted to the
  cluster

These are defensive controls for a demo/portfolio environment, not a
certification or compliance claim.
