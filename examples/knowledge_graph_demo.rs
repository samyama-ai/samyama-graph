//! Enterprise Knowledge Graph: Fortune 500 Tech Company
//!
//! Demonstrates Samyama's capabilities for enterprise knowledge management:
//! - **Graph Model:** Documents, Employees, Projects, Technologies
//! - **Vector Search:** Semantic document discovery (128-dim embeddings)
//! - **Graph Algorithms:** PageRank for knowledge hubs, WCC for topic clustering
//! - **Cypher Queries:** Cross-domain insight discovery
//!
//! No API keys required. All data is hardcoded and deterministic.

use samyama::graph::{GraphStore, Label, PropertyValue};
use samyama::vector::DistanceMetric;
use samyama::query::QueryEngine;
use samyama::algo::{build_view, page_rank, weakly_connected_components, PageRankConfig};
use std::collections::HashMap;

/// Generate a deterministic 128-dimensional mock embedding from a seed.
/// Uses a larger modulus to ensure uniqueness across 200+ documents.
fn mock_embedding(seed: usize) -> Vec<f32> {
    (0usize..128)
        .map(|j: usize| {
            // Use a large prime-based hash to avoid collisions across 200+ seeds
            let hash = (seed.wrapping_mul(2654435761) ^ j.wrapping_mul(40503)) % 10000;
            (hash as f32 / 10000.0).max(0.001) // Avoid exact zero
        })
        .collect()
}

/// Generate a query embedding that is similar to (but distinct from) mock_embedding(seed).
/// Adds a small perturbation so the HNSW index does not encounter exact-match distances.
fn query_embedding(seed: usize) -> Vec<f32> {
    (0usize..128)
        .map(|j: usize| {
            let hash = (seed.wrapping_mul(2654435761) ^ j.wrapping_mul(40503)) % 10000;
            let base = (hash as f32 / 10000.0).max(0.001);
            // Small deterministic perturbation to avoid exact match
            let offset = ((j * 3 + 1) % 50) as f32 / 50000.0;
            (base + offset).min(0.999)
        })
        .collect()
}

/// Helper to print a horizontal rule.
fn separator() {
    println!(
        "{}",
        "─".repeat(90)
    );
}

fn main() {
    println!();
    println!("╔══════════════════════════════════════════════════════════════════════════════════════════╗");
    println!("║              Enterprise Knowledge Graph  --  Fortune 500 Tech Company                   ║");
    println!("║              Powered by Samyama Graph Database                                          ║");
    println!("╚══════════════════════════════════════════════════════════════════════════════════════════╝");
    println!();

    let mut store = GraphStore::new();
    let engine = QueryEngine::new();

    // ════════════════════════════════════════════════════════════════════════════════════════
    // Step 1: Build the Enterprise Knowledge Graph
    // ════════════════════════════════════════════════════════════════════════════════════════
    println!("Step 1: Building Enterprise Knowledge Graph");
    separator();

    // -- Create vector index for document embeddings --
    store
        .create_vector_index("Document", "embedding", 128, DistanceMetric::Cosine)
        .unwrap();

    // ── Documents ───────────────────────────────────────────────────────────────────────────
    // Each document: (title, department, author_name, date, content_summary)
    let documents: Vec<(&str, &str, &str, &str, &str)> = vec![
        // Engineering (40 docs)
        ("Microservices Migration Guide v3.2", "Engineering", "Sarah Chen", "2025-01-15", "Step-by-step migration from monolith to microservices using Kubernetes and gRPC"),
        ("AWS Cost Optimization Playbook", "Engineering", "Marcus Johnson", "2025-02-10", "Strategies for reducing cloud spend by 40% through reserved instances and spot fleets"),
        ("API Gateway Design Patterns", "Engineering", "Sarah Chen", "2025-03-01", "Circuit breaker, rate limiting, and retry patterns for distributed API gateways"),
        ("Event-Driven Architecture Blueprint", "Engineering", "James Liu", "2025-01-20", "Kafka-based event sourcing and CQRS patterns for real-time data pipelines"),
        ("Database Sharding Strategy", "Engineering", "Priya Sharma", "2025-02-28", "Horizontal partitioning strategies for PostgreSQL at 10TB+ scale"),
        ("CI/CD Pipeline Standards v2.1", "Engineering", "Derek Williams", "2025-01-05", "GitHub Actions workflows with automated testing, security scanning, and canary deploys"),
        ("Kubernetes Cluster Hardening Guide", "Engineering", "Alex Rivera", "2025-03-12", "Pod security policies, network policies, and RBAC configuration for production clusters"),
        ("GraphQL Federation Architecture", "Engineering", "Sarah Chen", "2025-02-15", "Apollo Federation v2 setup for unified graph across 12 backend services"),
        ("Redis Caching Strategy", "Engineering", "James Liu", "2025-01-25", "Cache invalidation patterns, TTL strategies, and Redis Cluster topology for session management"),
        ("Observability Stack Design", "Engineering", "Marcus Johnson", "2025-03-08", "OpenTelemetry instrumentation with Grafana, Prometheus, and Jaeger tracing"),
        ("Zero-Downtime Deployment Guide", "Engineering", "Derek Williams", "2025-02-20", "Blue-green and rolling deployment strategies for 99.99% uptime SLA"),
        ("gRPC Service Mesh Configuration", "Engineering", "Alex Rivera", "2025-01-30", "Istio service mesh setup with mTLS, traffic splitting, and fault injection"),
        ("Data Lake Architecture v2.0", "Engineering", "Priya Sharma", "2025-03-15", "Medallion architecture on S3 with Delta Lake for ACID transactions on data lakes"),
        ("Frontend Performance Optimization", "Engineering", "Emily Tran", "2025-02-05", "Code splitting, lazy loading, and Core Web Vitals optimization for React applications"),
        ("Terraform Module Library v3.0", "Engineering", "Derek Williams", "2025-01-10", "Reusable Terraform modules for VPC, EKS, RDS, and IAM across 15 AWS accounts"),
        ("WebSocket Scaling Architecture", "Engineering", "James Liu", "2025-03-05", "Scaling real-time WebSocket connections to 1M concurrent users with Redis pub/sub"),
        ("Monorepo Build Optimization", "Engineering", "Emily Tran", "2025-02-25", "Nx workspace configuration with distributed caching and affected-based testing"),
        ("Service Discovery with Consul", "Engineering", "Alex Rivera", "2025-01-18", "HashiCorp Consul setup for dynamic service registration and health checking"),
        ("PostgreSQL Replication Setup", "Engineering", "Priya Sharma", "2025-03-20", "Streaming replication with automatic failover using Patroni and HAProxy"),
        ("Container Image Security", "Engineering", "Marcus Johnson", "2025-02-12", "Multi-stage builds, distroless images, and vulnerability scanning with Trivy"),
        // Product (20 docs)
        ("Q4 Product Roadmap - AI Features", "Product", "Lisa Park", "2025-01-08", "AI-powered search, recommendation engine, and automated content moderation roadmap"),
        ("User Research Report - Enterprise Dashboard", "Product", "Michael Torres", "2025-02-18", "Findings from 50 enterprise user interviews on dashboard customization needs"),
        ("Competitive Analysis - Cloud Platforms 2025", "Product", "Lisa Park", "2025-03-10", "Feature comparison of AWS, GCP, and Azure for enterprise AI/ML workloads"),
        ("Product-Led Growth Strategy", "Product", "Rachel Green", "2025-01-22", "Self-serve onboarding funnel optimization targeting 30% activation improvement"),
        ("Mobile App Redesign Spec v2.0", "Product", "Michael Torres", "2025-02-08", "iOS and Android redesign with accessibility focus and offline-first architecture"),
        ("API Monetization Framework", "Product", "Lisa Park", "2025-03-18", "Tiered pricing model for API consumption with usage-based billing integration"),
        ("Customer Journey Mapping - Onboarding", "Product", "Rachel Green", "2025-01-28", "End-to-end mapping of enterprise customer onboarding touchpoints and friction analysis"),
        ("Feature Flag Management Policy", "Product", "Michael Torres", "2025-02-22", "LaunchDarkly integration guidelines for progressive rollouts and A/B testing"),
        ("Internationalization Roadmap", "Product", "Rachel Green", "2025-03-02", "Localization strategy for 12 languages with RTL support and regional compliance"),
        ("Analytics Dashboard Requirements", "Product", "Lisa Park", "2025-01-15", "Real-time analytics with custom KPI tracking, anomaly detection, and executive reporting"),
        // Security (25 docs)
        ("SOC2 Compliance Audit Checklist", "Security", "David Kim", "2025-01-12", "Complete checklist for SOC2 Type II audit preparation covering all trust service criteria"),
        ("Incident Response Playbook v4.0", "Security", "Jennifer Walsh", "2025-02-14", "Step-by-step procedures for P1-P4 security incidents with escalation matrices"),
        ("Penetration Testing Report Q1 2025", "Security", "David Kim", "2025-03-25", "External and internal pen test findings with CVE analysis and remediation timeline"),
        ("Zero Trust Architecture Design", "Security", "Alex Rivera", "2025-01-20", "BeyondCorp-style zero trust implementation with identity-aware proxy and microsegmentation"),
        ("SIEM Configuration Guide", "Security", "Jennifer Walsh", "2025-02-28", "Splunk Enterprise Security configuration with custom correlation rules and threat intelligence"),
        ("Vulnerability Management Policy", "Security", "David Kim", "2025-03-08", "Automated CVE scanning, risk scoring, and SLA-driven patch management lifecycle"),
        ("Cloud Security Posture Management", "Security", "Jennifer Walsh", "2025-01-25", "AWS Config rules, Security Hub integration, and automated remediation workflows"),
        ("Data Classification Standard", "Security", "David Kim", "2025-02-10", "Four-tier data classification system with handling requirements for PII, PHI, and financial data"),
        ("Secret Management with Vault", "Security", "Alex Rivera", "2025-03-15", "HashiCorp Vault deployment for dynamic secrets, PKI, and encryption as a service"),
        ("Security Awareness Training Plan", "Security", "Jennifer Walsh", "2025-02-01", "Quarterly phishing simulations and role-based security training curriculum"),
        // HR (20 docs)
        ("Remote Work Policy v3.0", "HR", "Angela Foster", "2025-01-05", "Hybrid work guidelines including equipment stipend, home office requirements, and timezone policies"),
        ("Engineering Career Ladder v2.1", "HR", "Tom Bradley", "2025-02-20", "IC and management tracks from L3 to Distinguished Engineer with promotion criteria"),
        ("Diversity Equity and Inclusion Report 2024", "HR", "Angela Foster", "2025-01-18", "Annual DEI metrics, ERG impact analysis, and 2025 inclusion initiatives"),
        ("Stock Option Vesting Guide", "HR", "Tom Bradley", "2025-03-01", "Four-year vesting schedule details, exercise windows, and tax implications"),
        ("Performance Review Calibration Guide", "HR", "Angela Foster", "2025-02-15", "Manager calibration process for bi-annual performance reviews with rating distribution"),
        ("New Hire Onboarding Checklist", "HR", "Tom Bradley", "2025-01-28", "30-60-90 day onboarding milestones for engineering, product, and design roles"),
        ("Employee Handbook 2025 Edition", "HR", "Angela Foster", "2025-03-10", "Company policies, benefits overview, code of conduct, and grievance procedures"),
        ("Interview Process Standardization", "HR", "Tom Bradley", "2025-02-08", "Structured interview rubrics with bias-reduction techniques and scoring guidelines"),
        ("Parental Leave Policy Update", "HR", "Angela Foster", "2025-01-22", "Enhanced 20-week parental leave policy with flexible return-to-work arrangements"),
        ("Compensation Benchmarking Report", "HR", "Tom Bradley", "2025-03-18", "Market rate analysis across 8 tech hubs with equity band adjustments"),
        // Legal (20 docs)
        ("GDPR Data Deletion Policy", "Legal", "Robert Nguyen", "2025-01-10", "Right to erasure implementation with data mapping, deletion workflows, and audit trails"),
        ("CCPA Compliance Framework", "Legal", "Patricia Hernandez", "2025-02-22", "California Consumer Privacy Act compliance including opt-out mechanisms and data inventory"),
        ("Third-Party Vendor Risk Assessment", "Legal", "Robert Nguyen", "2025-03-05", "Vendor due diligence framework with security questionnaire and contract requirements"),
        ("Open Source License Compliance", "Legal", "Patricia Hernandez", "2025-01-30", "License compatibility matrix and approval workflow for AGPL, GPL, MIT, and Apache 2.0"),
        ("Data Processing Agreement Template", "Legal", "Robert Nguyen", "2025-02-18", "Standard DPA template for sub-processors with SCCs and data transfer impact assessment"),
        ("Intellectual Property Policy", "Legal", "Patricia Hernandez", "2025-03-12", "Patent filing process, invention disclosure, and IP ownership for employee innovations"),
        ("AI Ethics and Governance Framework", "Legal", "Robert Nguyen", "2025-01-25", "Responsible AI principles, bias auditing requirements, and model governance lifecycle"),
        ("Export Control Compliance Guide", "Legal", "Patricia Hernandez", "2025-02-05", "EAR and ITAR compliance for software distribution across 40 countries"),
        ("Contractual SLA Standards", "Legal", "Robert Nguyen", "2025-03-20", "Standard SLA tiers for enterprise contracts with penalty and credit calculations"),
        ("Privacy Impact Assessment Template", "Legal", "Patricia Hernandez", "2025-02-28", "PIA template for new features processing personal data with DPO review workflow"),
        // Data Science (15 docs)
        ("ML Model Governance Handbook", "Data Science", "Wei Zhang", "2025-01-12", "Model registry, versioning, A/B testing, and production monitoring for ML models"),
        ("Feature Store Architecture", "Data Science", "Aisha Okafor", "2025-02-25", "Feast-based feature store for online and offline feature serving at scale"),
        ("A/B Testing Statistical Framework", "Data Science", "Wei Zhang", "2025-03-08", "Bayesian and frequentist test design with power analysis and guardrail metrics"),
        ("NLP Pipeline Design Document", "Data Science", "Aisha Okafor", "2025-01-20", "Transformer-based NLP pipeline for entity extraction, sentiment, and summarization"),
        ("Recommendation Engine Architecture", "Data Science", "Wei Zhang", "2025-02-15", "Collaborative filtering and content-based hybrid model serving 50M daily recommendations"),
        ("Data Quality Monitoring Framework", "Data Science", "Aisha Okafor", "2025-03-18", "Great Expectations integration for automated data validation and anomaly detection"),
        ("Time Series Forecasting Playbook", "Data Science", "Wei Zhang", "2025-01-28", "Prophet and neural forecasting models for demand prediction and capacity planning"),
        ("MLOps Infrastructure Guide", "Data Science", "Aisha Okafor", "2025-02-10", "Kubeflow pipelines, experiment tracking with MLflow, and GPU cluster management"),
        // DevOps (15 docs)
        ("Infrastructure as Code Standards", "DevOps", "Carlos Mendez", "2025-01-08", "Terraform and Pulumi conventions for multi-region infrastructure deployment"),
        ("Disaster Recovery Plan v2.5", "DevOps", "Nina Petrov", "2025-02-20", "RPO/RTO targets, failover procedures, and annual DR drill runbook"),
        ("Log Aggregation Architecture", "DevOps", "Carlos Mendez", "2025-03-12", "ELK stack configuration with log rotation, retention policies, and alerting rules"),
        ("SRE On-Call Handbook", "DevOps", "Nina Petrov", "2025-01-22", "On-call rotation, escalation procedures, post-mortem templates, and toil reduction strategies"),
        ("Secrets Rotation Automation", "DevOps", "Carlos Mendez", "2025-02-08", "Automated credential rotation for databases, API keys, and TLS certificates"),
        ("Cost Tagging and Allocation Guide", "DevOps", "Nina Petrov", "2025-03-01", "AWS cost allocation tags, FinOps dashboards, and chargeback model for 20 teams"),
        ("Chaos Engineering Playbook", "DevOps", "Carlos Mendez", "2025-01-30", "Gremlin and Litmus chaos experiments for validating resilience of critical services"),
        ("CDN Configuration and Tuning", "DevOps", "Nina Petrov", "2025-02-18", "CloudFront distribution setup with cache behavior optimization and origin failover"),
        // Architecture (20 docs)
        ("Platform Architecture Decision Record Index", "Architecture", "Sarah Chen", "2025-01-05", "Master index of 150+ architecture decision records covering all platform domains"),
        ("Event Mesh Design for Multi-Region", "Architecture", "James Liu", "2025-02-14", "Cross-region event mesh with Kafka MirrorMaker 2 and conflict resolution strategies"),
        ("API Versioning Strategy", "Architecture", "Sarah Chen", "2025-03-10", "URL-based and header-based API versioning with deprecation policy and migration tooling"),
        ("Domain-Driven Design Reference", "Architecture", "James Liu", "2025-01-18", "Bounded context mapping, aggregate design, and anti-corruption layer patterns"),
        ("Multi-Tenancy Architecture v2.0", "Architecture", "Sarah Chen", "2025-02-25", "Tenant isolation patterns with shared-nothing and pool models for SaaS platform"),
        ("CQRS Implementation Guide", "Architecture", "James Liu", "2025-03-05", "Command query responsibility segregation with event sourcing for audit-ready systems"),
        ("Edge Computing Strategy", "Architecture", "Alex Rivera", "2025-01-25", "CloudFlare Workers and AWS Lambda@Edge for latency-sensitive workloads"),
        ("Data Mesh Governance Framework", "Architecture", "Priya Sharma", "2025-02-08", "Data product ownership, federated governance, and self-serve data platform design"),
        // Engineering - additional (30 more -> total 50)
        ("Memory Safety Audit Report", "Engineering", "Sarah Chen", "2025-04-01", "Rust migration analysis showing 73% reduction in memory-related CVEs across services"),
        ("Load Testing Framework v2.0", "Engineering", "Marcus Johnson", "2025-04-05", "K6 and Gatling test suites for simulating 500K concurrent users on Atlas Platform"),
        ("Error Budget Policy", "Engineering", "Derek Williams", "2025-04-10", "SLO-based error budgets with automated release gating and reliability scoring"),
        ("Feature Toggle Architecture", "Engineering", "Emily Tran", "2025-04-12", "Server-side and client-side toggle patterns with kill switch and gradual rollout"),
        ("Async Message Processing Guide", "Engineering", "James Liu", "2025-04-15", "Dead letter queue handling, retry strategies, and idempotency for Kafka consumers"),
        ("Database Migration Runbook v3.0", "Engineering", "Priya Sharma", "2025-04-18", "Zero-downtime schema migration with gh-ost and pt-online-schema-change tooling"),
        ("Browser Compatibility Matrix", "Engineering", "Emily Tran", "2025-04-20", "Supported browser versions, polyfill strategy, and progressive enhancement guidelines"),
        ("Rate Limiting Design v2.1", "Engineering", "Alex Rivera", "2025-04-22", "Token bucket and sliding window algorithms with Redis-backed distributed rate limiting"),
        ("Service Level Objectives Catalog", "Engineering", "Marcus Johnson", "2025-04-25", "SLO definitions for 45 production services with error budget burn rate alerts"),
        ("Code Review Standards v2.0", "Engineering", "Sarah Chen", "2025-04-28", "Review checklist, approval policies, and automated code quality gates for PRs"),
        ("Dependency Management Policy", "Engineering", "Derek Williams", "2025-05-01", "Dependabot configuration, vulnerability scanning, and upgrade cadence requirements"),
        ("Distributed Tracing Implementation", "Engineering", "Marcus Johnson", "2025-05-03", "OpenTelemetry trace propagation across 30 microservices with custom span attributes"),
        ("API Pagination Standards", "Engineering", "James Liu", "2025-05-05", "Cursor-based and offset pagination patterns with consistent response envelope format"),
        ("State Machine Design Patterns", "Engineering", "Priya Sharma", "2025-05-08", "Event-driven state machines for order processing, approval workflows, and deployments"),
        ("Webhook Delivery System Design", "Engineering", "Alex Rivera", "2025-05-10", "At-least-once webhook delivery with exponential backoff and signature verification"),
        ("Image Processing Pipeline", "Engineering", "Emily Tran", "2025-05-12", "CDN-based image optimization with WebP conversion, lazy loading, and responsive sizing"),
        ("Internal Developer Portal Spec", "Engineering", "Derek Williams", "2025-05-15", "Backstage-based developer portal with service catalog, API docs, and golden paths"),
        ("Socket.IO Cluster Configuration", "Engineering", "James Liu", "2025-05-18", "Sticky sessions with Redis adapter for real-time collaboration across 8 server instances"),
        ("Build Artifact Management", "Engineering", "Marcus Johnson", "2025-05-20", "Artifactory setup with retention policies, promotion pipelines, and vulnerability scanning"),
        ("Technical Debt Inventory Q1 2025", "Engineering", "Sarah Chen", "2025-05-22", "Prioritized list of 89 tech debt items with effort estimates and business impact scores"),
        ("GraphQL Schema Design Guide", "Engineering", "Alex Rivera", "2025-05-25", "Schema-first design, resolver patterns, and N+1 query prevention with DataLoader"),
        ("Canary Release Automation", "Engineering", "Derek Williams", "2025-05-28", "Flagger-based canary analysis with custom metrics, automated rollback, and traffic splitting"),
        ("Rust Service Template v1.0", "Engineering", "Sarah Chen", "2025-06-01", "Production-ready Rust microservice template with tracing, health checks, and graceful shutdown"),
        ("Event Schema Registry", "Engineering", "James Liu", "2025-06-03", "Confluent Schema Registry for Avro and Protobuf event schemas with compatibility checks"),
        ("Database Connection Pooling Guide", "Engineering", "Priya Sharma", "2025-06-05", "PgBouncer and HikariCP configuration for optimal connection management under high load"),
        ("Frontend Testing Strategy v2.0", "Engineering", "Emily Tran", "2025-06-08", "Testing pyramid with Vitest unit tests, Playwright E2E, and visual regression testing"),
        ("Network Segmentation Design", "Engineering", "Alex Rivera", "2025-06-10", "VPC peering, transit gateway, and microsegmentation for production network isolation"),
        ("Incident Post-Mortem Template v3", "Engineering", "Marcus Johnson", "2025-06-12", "Blameless post-mortem format with timeline, root cause, and corrective action tracking"),
        ("API SDK Generation Pipeline", "Engineering", "Derek Williams", "2025-06-15", "OpenAPI-based SDK generation for Python, TypeScript, Go, and Java client libraries"),
        ("Search Indexing Architecture", "Engineering", "Priya Sharma", "2025-06-18", "Elasticsearch indexing pipeline with custom analyzers, synonyms, and relevance tuning"),
        // Product - additional (15 more -> total 25)
        ("Voice of Customer Report Q1 2025", "Product", "Michael Torres", "2025-04-02", "NPS analysis and feature request aggregation from 2000 enterprise customer touchpoints"),
        ("Pricing Tier Restructuring Proposal", "Product", "Lisa Park", "2025-04-08", "Three-tier to usage-based pricing migration with grandfathering and upgrade incentives"),
        ("Design System v3.0 Specification", "Product", "Rachel Green", "2025-04-14", "Component library with accessibility tokens, dark mode, and responsive breakpoints"),
        ("Platform Ecosystem Strategy", "Product", "Lisa Park", "2025-04-20", "Marketplace integration, partner APIs, and third-party extension framework roadmap"),
        ("User Segmentation Model", "Product", "Michael Torres", "2025-04-25", "Behavioral cohort analysis with predictive churn scoring and engagement tiers"),
        ("Notification System Requirements", "Product", "Rachel Green", "2025-05-01", "Multi-channel notification framework with preference management and delivery analytics"),
        ("Accessibility Compliance Report", "Product", "Michael Torres", "2025-05-08", "WCAG 2.1 AA compliance audit findings with remediation plan for 12 critical issues"),
        ("Beta Program Management Guide", "Product", "Lisa Park", "2025-05-15", "Feature flag-based beta rollout with feedback collection and success metric tracking"),
        ("Content Management System Spec", "Product", "Rachel Green", "2025-05-20", "Headless CMS architecture for multi-brand content with workflow approval and versioning"),
        ("Customer Success Playbook", "Product", "Michael Torres", "2025-05-25", "Health score model, risk detection triggers, and intervention playbook for enterprise accounts"),
        ("Product Analytics Implementation", "Product", "Lisa Park", "2025-06-01", "Amplitude and Mixpanel instrumentation guide with taxonomy and governance framework"),
        ("White-Label Configuration Guide", "Product", "Rachel Green", "2025-06-05", "Theme customization, branded domains, and tenant-specific feature configuration"),
        ("Developer Experience Survey Results", "Product", "Michael Torres", "2025-06-10", "API usability study findings from 200 third-party developers with satisfaction benchmarks"),
        ("Compliance Dashboard Requirements", "Product", "Lisa Park", "2025-06-15", "Real-time compliance status tracking across SOC2, GDPR, HIPAA, and PCI-DSS frameworks"),
        ("Customer Data Platform Spec", "Product", "Rachel Green", "2025-06-18", "Unified customer profile with identity resolution, consent management, and data activation"),
        // Security - additional (15 more -> total 25)
        ("Bug Bounty Program Report 2024", "Security", "David Kim", "2025-04-05", "Annual bug bounty statistics: 47 valid submissions, 3 critical, $180K total payouts"),
        ("API Security Assessment v2.0", "Security", "Jennifer Walsh", "2025-04-12", "OWASP API Top 10 assessment with JWT validation, input sanitization, and rate limiting"),
        ("Encryption Standards Guide", "Security", "David Kim", "2025-04-18", "AES-256-GCM for data at rest, TLS 1.3 enforcement, and key management lifecycle"),
        ("Identity and Access Management Design", "Security", "Jennifer Walsh", "2025-04-25", "Okta-based SSO with SCIM provisioning, MFA enforcement, and conditional access policies"),
        ("Container Runtime Security", "Security", "David Kim", "2025-05-02", "Falco runtime detection, seccomp profiles, and AppArmor policies for Kubernetes pods"),
        ("Third-Party Integration Security Review", "Security", "Jennifer Walsh", "2025-05-10", "Security assessment framework for SaaS integrations with data flow analysis"),
        ("Network Intrusion Detection Setup", "Security", "David Kim", "2025-05-18", "Suricata IDS configuration with custom rules for detecting lateral movement patterns"),
        ("Supply Chain Security Policy", "Security", "Jennifer Walsh", "2025-05-25", "SLSA Level 3 compliance with Sigstore signing, SBOM generation, and provenance tracking"),
        ("Red Team Exercise Report Q1", "Security", "David Kim", "2025-06-01", "Internal red team findings: 12 attack paths identified, 8 remediated, 4 in progress"),
        ("DDoS Protection Architecture", "Security", "Jennifer Walsh", "2025-06-08", "Multi-layer DDoS mitigation with CloudFlare, AWS Shield, and application-level throttling"),
        // HR - additional (10 more -> total 20)
        ("Employee Engagement Survey 2025", "HR", "Angela Foster", "2025-04-01", "Annual engagement results: 78% favorable, key drivers and action planning framework"),
        ("Technical Interview Rubric v3.0", "HR", "Tom Bradley", "2025-04-10", "Standardized evaluation criteria for system design, coding, and behavioral interviews"),
        ("Wellness Program Overview", "HR", "Angela Foster", "2025-04-20", "Mental health resources, fitness reimbursement, and ergonomic assessment program details"),
        ("Manager Training Curriculum", "HR", "Tom Bradley", "2025-05-01", "Leadership development program covering coaching, feedback, and difficult conversations"),
        ("Contractor Onboarding Checklist", "HR", "Angela Foster", "2025-05-10", "Security clearance, NDA execution, and system access provisioning for contingent workers"),
        ("Knowledge Transfer Protocol", "HR", "Tom Bradley", "2025-05-20", "Structured knowledge transfer process for departing employees and role transitions"),
        ("Team Topology Guidelines", "HR", "Angela Foster", "2025-06-01", "Conway's law alignment with stream-aligned, platform, and enabling team structures"),
        ("Return to Office Policy v2.0", "HR", "Tom Bradley", "2025-06-08", "Updated hybrid policy with core collaboration days and remote work eligibility criteria"),
        ("Sabbatical Leave Program", "HR", "Angela Foster", "2025-06-12", "Six-week sabbatical eligibility after 5 years with project handoff requirements"),
        ("Internal Mobility Framework", "HR", "Tom Bradley", "2025-06-18", "Job posting, lateral transfer, and rotation program guidelines with manager approval"),
        // Legal - additional (10 more -> total 20)
        ("Anti-Bribery and Corruption Policy", "Legal", "Robert Nguyen", "2025-04-05", "FCPA and UK Bribery Act compliance with gift reporting and third-party due diligence"),
        ("Software License Audit Preparation", "Legal", "Patricia Hernandez", "2025-04-15", "BSA audit readiness checklist with software inventory and license reconciliation"),
        ("Incident Notification Requirements", "Legal", "Robert Nguyen", "2025-04-25", "Regulatory breach notification timelines across GDPR, CCPA, HIPAA, and SEC requirements"),
        ("Terms of Service Update v5.0", "Legal", "Patricia Hernandez", "2025-05-05", "Updated ToS covering AI-generated content, data portability, and service level guarantees"),
        ("Employee Non-Compete Analysis", "Legal", "Robert Nguyen", "2025-05-15", "State-by-state enforceability analysis of non-compete and non-solicitation agreements"),
        ("Regulatory Change Impact Assessment", "Legal", "Patricia Hernandez", "2025-05-25", "EU AI Act and US state privacy law impact analysis with compliance gap assessment"),
        ("Litigation Hold Protocol", "Legal", "Robert Nguyen", "2025-06-01", "Data preservation procedures, custodian notification, and collection workflow for legal holds"),
        ("Insurance Coverage Review 2025", "Legal", "Patricia Hernandez", "2025-06-08", "Cyber liability, D&O, and E&O coverage analysis with gap recommendations"),
        ("Accessibility Legal Requirements", "Legal", "Robert Nguyen", "2025-06-12", "ADA Title III, Section 508, and EAA compliance obligations for digital products"),
        ("Cross-Border Data Transfer Guide", "Legal", "Patricia Hernandez", "2025-06-18", "Standard contractual clauses, data transfer impact assessments, and adequacy decisions"),
        // Data Science - additional (12 more -> total 20)
        ("Experiment Platform Architecture", "Data Science", "Wei Zhang", "2025-04-02", "Centralized experimentation platform with multi-arm bandit and contextual targeting"),
        ("Bias Detection Framework", "Data Science", "Aisha Okafor", "2025-04-12", "Fairness metrics, demographic parity testing, and model card generation for ML systems"),
        ("Data Catalog Implementation", "Data Science", "Wei Zhang", "2025-04-22", "Apache Atlas integration with automated metadata extraction and data lineage tracking"),
        ("GPU Cluster Cost Analysis", "Data Science", "Aisha Okafor", "2025-05-02", "A100 vs H100 cost-performance comparison for training and inference workloads"),
        ("Customer Lifetime Value Model", "Data Science", "Wei Zhang", "2025-05-12", "Probabilistic CLV model with BG/NBD and Gamma-Gamma sub-models for revenue prediction"),
        ("Data Privacy Engineering Guide", "Data Science", "Aisha Okafor", "2025-05-22", "Differential privacy, synthetic data generation, and k-anonymity for ML training data"),
        ("Real-Time Scoring Architecture", "Data Science", "Wei Zhang", "2025-06-01", "Sub-10ms model serving with TensorRT, ONNX Runtime, and feature caching strategies"),
        ("Anomaly Detection Playbook", "Data Science", "Aisha Okafor", "2025-06-08", "Isolation forest and autoencoder-based anomaly detection for fraud and infrastructure"),
        ("Knowledge Graph Embedding Study", "Data Science", "Wei Zhang", "2025-06-12", "TransE and RotatE benchmarks for product recommendation and entity linking tasks"),
        ("Data Pipeline SLA Dashboard", "Data Science", "Aisha Okafor", "2025-06-15", "Airflow DAG monitoring with freshness SLAs, quality checks, and alerting workflows"),
        ("Causal Inference Handbook", "Data Science", "Wei Zhang", "2025-06-18", "Propensity scoring, instrumental variables, and double ML for causal effect estimation"),
        ("LLM Fine-Tuning Playbook", "Data Science", "Aisha Okafor", "2025-06-20", "LoRA and QLoRA fine-tuning recipes for domain-specific language models on internal data"),
        // DevOps - additional (12 more -> total 20)
        ("Multi-Cloud Strategy Document", "DevOps", "Carlos Mendez", "2025-04-05", "AWS primary, GCP secondary architecture with Terraform abstraction and failover procedures"),
        ("Kubernetes Upgrade Runbook v1.28", "DevOps", "Nina Petrov", "2025-04-15", "Rolling upgrade procedure for EKS clusters with addon compatibility and rollback steps"),
        ("GitOps Workflow Implementation", "DevOps", "Carlos Mendez", "2025-04-25", "ArgoCD-based GitOps with progressive delivery, drift detection, and audit logging"),
        ("Database Backup and Recovery Plan", "DevOps", "Nina Petrov", "2025-05-05", "Automated backup verification, point-in-time recovery testing, and cross-region replication"),
        ("Service Mesh Performance Tuning", "DevOps", "Carlos Mendez", "2025-05-15", "Istio sidecar resource optimization, connection pooling, and mTLS performance impact"),
        ("Capacity Planning Model v2.0", "DevOps", "Nina Petrov", "2025-05-25", "Predictive scaling model with seasonal adjustments and reserved instance optimization"),
        ("Incident Management Tool Setup", "DevOps", "Carlos Mendez", "2025-06-01", "PagerDuty configuration with intelligent routing, auto-escalation, and Slack integration"),
        ("Container Registry Management", "DevOps", "Nina Petrov", "2025-06-05", "ECR lifecycle policies, image signing, and cross-account replication for 15 AWS accounts"),
        ("Platform Health Dashboard Design", "DevOps", "Carlos Mendez", "2025-06-10", "Executive SLA dashboard with four golden signals and business impact correlation"),
        ("FinOps Maturity Assessment", "DevOps", "Nina Petrov", "2025-06-15", "Cloud financial management maturity across crawl-walk-run phases with tool recommendations"),
        ("Zero-Touch Provisioning Guide", "DevOps", "Carlos Mendez", "2025-06-18", "Automated developer environment setup with Terraform, Ansible, and custom CLI tooling"),
        ("Production Readiness Review Checklist", "DevOps", "Nina Petrov", "2025-06-20", "40-point checklist for production launch covering observability, security, and scalability"),
        // Architecture - additional (12 more -> total 20)
        ("Strangler Fig Migration Pattern", "Architecture", "Sarah Chen", "2025-04-02", "Incremental monolith decomposition with routing layer, feature parity tracking, and cutover"),
        ("Saga Pattern Implementation Guide", "Architecture", "James Liu", "2025-04-12", "Orchestration and choreography saga patterns for distributed transaction management"),
        ("Cache Architecture Decision Record", "Architecture", "Sarah Chen", "2025-04-22", "Multi-tier caching strategy with CDN, application, and database query cache layers"),
        ("Sidecar Pattern Reference", "Architecture", "Alex Rivera", "2025-05-02", "Envoy sidecar configuration for observability injection, auth, and traffic management"),
        ("Event Sourcing Reference Architecture", "Architecture", "James Liu", "2025-05-12", "Complete event sourcing implementation with snapshots, projections, and temporal queries"),
        ("Bulkhead Pattern Implementation", "Architecture", "Sarah Chen", "2025-05-22", "Thread pool isolation and circuit breaker patterns for resilient microservice communication"),
        ("API Composition Pattern Guide", "Architecture", "James Liu", "2025-06-01", "Backend-for-frontend and API gateway composition for mobile and web client optimization"),
        ("Observability Reference Architecture", "Architecture", "Alex Rivera", "2025-06-05", "Three pillars of observability with correlation IDs, structured logging, and custom metrics"),
        ("Data Replication Strategy", "Architecture", "Priya Sharma", "2025-06-10", "CDC with Debezium, read replica routing, and eventual consistency handling patterns"),
        ("Security Architecture Review Process", "Architecture", "Sarah Chen", "2025-06-15", "Threat modeling with STRIDE, security architecture review board, and risk acceptance"),
        ("Polyglot Persistence Guide", "Architecture", "James Liu", "2025-06-18", "Database selection criteria for OLTP, OLAP, graph, time-series, and document workloads"),
        ("Platform Reliability Engineering", "Architecture", "Alex Rivera", "2025-06-20", "Reliability patterns including retry, timeout, bulkhead, and circuit breaker at platform level"),
        // Additional cross-functional documents (6 more to reach 201+)
        ("Technical Writing Style Guide", "Engineering", "Diana Lopez", "2025-06-22", "Documentation standards for API docs, architecture decision records, and runbooks"),
        ("Open Source Contribution Policy", "Legal", "Robert Nguyen", "2025-06-25", "CLA requirements, license compatibility, and approval process for external contributions"),
        ("Platform Security Metrics Dashboard", "Security", "David Kim", "2025-06-28", "MTTD, MTTR, vulnerability density, and patch compliance tracking across all services"),
        ("Data Retention Policy v3.0", "Legal", "Patricia Hernandez", "2025-07-01", "Retention schedules by data classification with automated lifecycle management and legal holds"),
        ("Engineering Productivity Report Q2", "Engineering", "Victoria Patel", "2025-07-05", "Build times, deploy frequency, lead time, and developer satisfaction metrics across 20 teams"),
        ("AI Model Registry Specification", "Data Science", "Wei Zhang", "2025-07-08", "Centralized model registry with versioning, lineage tracking, and deployment approval workflows"),
    ];

    // Track document NodeIds for later use
    let mut doc_ids: Vec<u64> = Vec::new();
    let mut doc_titles: HashMap<u64, String> = HashMap::new();

    for (i, (title, dept, _author, date, summary)) in documents.iter().enumerate() {
        let nid = store.create_node("Document");
        if let Some(node) = store.get_node_mut(nid) {
            node.set_property("title", *title);
            node.set_property("department", *dept);
            node.set_property("date", *date);
            node.set_property("content_summary", *summary);
        }
        // Set embedding via store method (required for vector index)
        let emb = mock_embedding(i);
        store
            .set_node_property("default", nid, "embedding", PropertyValue::Vector(emb))
            .unwrap();
        doc_ids.push(nid.as_u64());
        doc_titles.insert(nid.as_u64(), title.to_string());
    }

    println!(
        "  Created {} documents across {} departments",
        documents.len(),
        ["Engineering", "Product", "Security", "HR", "Legal", "Data Science", "DevOps", "Architecture"].len()
    );

    // ── Employees ──────────────────────────────────────────────────────────────────────────
    let employees: Vec<(&str, &str, &str)> = vec![
        // (name, title, department)
        ("Sarah Chen", "Principal Engineer", "Engineering"),
        ("Marcus Johnson", "Staff Engineer", "Engineering"),
        ("James Liu", "Senior Architect", "Engineering"),
        ("Priya Sharma", "Database Lead", "Engineering"),
        ("Derek Williams", "DevOps Manager", "Engineering"),
        ("Alex Rivera", "Security Engineer", "Engineering"),
        ("Emily Tran", "Frontend Lead", "Engineering"),
        ("Kevin Park", "Backend Engineer", "Engineering"),
        ("Natasha Volkov", "SRE Lead", "Engineering"),
        ("Omar Hassan", "Platform Engineer", "Engineering"),
        ("Lisa Park", "VP Product", "Product"),
        ("Michael Torres", "Senior PM", "Product"),
        ("Rachel Green", "Growth PM", "Product"),
        ("Daniel Kim", "Product Analyst", "Product"),
        ("Sofia Martinez", "UX Researcher", "Product"),
        ("David Kim", "CISO", "Security"),
        ("Jennifer Walsh", "Security Architect", "Security"),
        ("Ryan Foster", "Security Analyst", "Security"),
        ("Mia Chen", "AppSec Engineer", "Security"),
        ("Tyler Brooks", "Compliance Analyst", "Security"),
        ("Angela Foster", "VP People", "HR"),
        ("Tom Bradley", "HR Business Partner", "HR"),
        ("Jessica Huang", "Talent Acquisition Lead", "HR"),
        ("Robert Nguyen", "General Counsel", "Legal"),
        ("Patricia Hernandez", "Privacy Counsel", "Legal"),
        ("Andrew Thompson", "Contract Attorney", "Legal"),
        ("Wei Zhang", "Principal Data Scientist", "Data Science"),
        ("Aisha Okafor", "ML Engineer", "Data Science"),
        ("Chris Taylor", "Data Analyst", "Data Science"),
        ("Lauren Mitchell", "Data Engineer", "Data Science"),
        ("Carlos Mendez", "Infrastructure Lead", "DevOps"),
        ("Nina Petrov", "SRE Manager", "DevOps"),
        ("Jason Lee", "Cloud Engineer", "DevOps"),
        ("Amanda Ross", "Release Engineer", "DevOps"),
        ("Benjamin Cole", "Staff Architect", "Architecture"),
        ("Stephanie Wright", "Solutions Architect", "Architecture"),
        ("Nathan Brooks", "Enterprise Architect", "Architecture"),
        ("Diana Lopez", "Technical Writer", "Engineering"),
        ("Samuel Osei", "QA Lead", "Engineering"),
        ("Victoria Patel", "Engineering Manager", "Engineering"),
        ("Thomas Wright", "CTO", "Engineering"),
        ("Hannah Kim", "Product Designer", "Product"),
        ("Jack Robinson", "API Product Manager", "Product"),
        ("Olivia Stone", "Content Strategist", "Product"),
        ("Grace Liu", "Threat Intelligence", "Security"),
        ("Ethan Cooper", "Network Security", "Security"),
        ("Isabella Russo", "Learning & Development", "HR"),
        ("Matthew Day", "Benefits Specialist", "HR"),
        ("Sophia Anderson", "Regulatory Counsel", "Legal"),
        ("Lucas Chen", "Research Scientist", "Data Science"),
    ];

    let mut emp_ids: Vec<u64> = Vec::new();
    let mut emp_names: HashMap<u64, String> = HashMap::new();
    let mut name_to_emp_id: HashMap<String, u64> = HashMap::new();

    for (name, title, dept) in &employees {
        let nid = store.create_node("Employee");
        if let Some(node) = store.get_node_mut(nid) {
            node.set_property("name", *name);
            node.set_property("title", *title);
            node.set_property("department", *dept);
        }
        emp_ids.push(nid.as_u64());
        emp_names.insert(nid.as_u64(), name.to_string());
        name_to_emp_id.insert(name.to_string(), nid.as_u64());
    }

    println!("  Created {} employees across departments", employees.len());

    // ── Projects ───────────────────────────────────────────────────────────────────────────
    let projects: Vec<(&str, &str, &str)> = vec![
        // (name, status, lead_department)
        ("Atlas Platform", "Active", "Engineering"),
        ("Phoenix Migration", "Active", "Engineering"),
        ("Quantum ML Pipeline", "Active", "Data Science"),
        ("Sentinel Security Suite", "Active", "Security"),
        ("Horizon Mobile App", "Active", "Product"),
        ("Titan Infrastructure", "Active", "DevOps"),
        ("Nebula Analytics", "Active", "Data Science"),
        ("Guardian Compliance", "Active", "Legal"),
        ("Pulse Employee Experience", "Active", "HR"),
        ("Apex API Platform", "Active", "Engineering"),
        ("Catalyst Data Mesh", "Active", "Architecture"),
        ("Forge CI/CD Pipeline", "Active", "DevOps"),
        ("Prism Observability", "Active", "Engineering"),
        ("Shield Zero Trust", "Active", "Security"),
        ("Echo Event Platform", "Active", "Engineering"),
        ("Compass Navigation", "Active", "Product"),
        ("Bridge Integration Hub", "Active", "Architecture"),
        ("Lighthouse Monitoring", "Active", "DevOps"),
        ("Orbit Multi-Region", "Active", "Architecture"),
        ("Vertex Graph Platform", "Active", "Engineering"),
        ("Aurora AI Features", "Active", "Data Science"),
        ("Bastion Access Control", "Active", "Security"),
        ("Crystal Reports Engine", "Active", "Product"),
        ("Delta Data Lake", "Active", "Engineering"),
        ("Ember Feature Flags", "Active", "Product"),
        ("Falcon Threat Detection", "Active", "Security"),
        ("Genesis Onboarding", "Active", "HR"),
        ("Helix Search Platform", "Active", "Engineering"),
        ("Iron Cache Layer", "Active", "Engineering"),
        ("Jade Documentation", "Active", "Engineering"),
    ];

    let mut proj_ids: Vec<u64> = Vec::new();
    let mut proj_names: HashMap<u64, String> = HashMap::new();

    for (name, status, dept) in &projects {
        let nid = store.create_node("Project");
        if let Some(node) = store.get_node_mut(nid) {
            node.set_property("name", *name);
            node.set_property("status", *status);
            node.set_property("department", *dept);
        }
        proj_ids.push(nid.as_u64());
        proj_names.insert(nid.as_u64(), name.to_string());
    }

    println!("  Created {} projects", projects.len());

    // ── Technologies ───────────────────────────────────────────────────────────────────────
    let technologies: Vec<(&str, &str)> = vec![
        // (name, category)
        ("Kubernetes", "Orchestration"),
        ("PostgreSQL", "Database"),
        ("Redis", "Cache"),
        ("Kafka", "Messaging"),
        ("Terraform", "IaC"),
        ("React", "Frontend"),
        ("gRPC", "Communication"),
        ("Docker", "Container"),
        ("Prometheus", "Monitoring"),
        ("Grafana", "Observability"),
        ("Istio", "Service Mesh"),
        ("GraphQL", "API"),
        ("Elasticsearch", "Search"),
        ("S3", "Storage"),
        ("Delta Lake", "Data Lake"),
        ("Spark", "Processing"),
        ("MLflow", "ML Ops"),
        ("Vault", "Secrets"),
        ("Splunk", "SIEM"),
        ("CloudFront", "CDN"),
        ("OpenTelemetry", "Tracing"),
        ("Consul", "Discovery"),
        ("Feast", "Feature Store"),
        ("Trivy", "Security Scan"),
    ];

    let mut tech_ids: Vec<u64> = Vec::new();
    let mut tech_names: HashMap<u64, String> = HashMap::new();
    let mut name_to_tech_id: HashMap<String, u64> = HashMap::new();

    for (name, category) in &technologies {
        let nid = store.create_node("Technology");
        if let Some(node) = store.get_node_mut(nid) {
            node.set_property("name", *name);
            node.set_property("category", *category);
        }
        tech_ids.push(nid.as_u64());
        tech_names.insert(nid.as_u64(), name.to_string());
        name_to_tech_id.insert(name.to_string(), nid.as_u64());
    }

    println!("  Created {} technologies", technologies.len());

    // ── Relationships ──────────────────────────────────────────────────────────────────────
    let mut edge_count = 0u64;

    // AUTHORED: Employee -> Document (based on author field in document data)
    for (i, (_title, _dept, author, _date, _summary)) in documents.iter().enumerate() {
        if let Some(&emp_raw) = name_to_emp_id.get(*author) {
            let emp_nid = samyama::graph::NodeId::new(emp_raw);
            let doc_nid = samyama::graph::NodeId::new(doc_ids[i]);
            store.create_edge(emp_nid, doc_nid, "AUTHORED").unwrap();
            edge_count += 1;
        }
    }

    // REFERENCES: Document -> Document (cross-references between related docs)
    let reference_pairs: Vec<(usize, usize)> = vec![
        (0, 3),   // Microservices Migration -> Event-Driven Architecture
        (0, 11),  // Microservices Migration -> gRPC Service Mesh
        (1, 14),  // AWS Cost Optimization -> Terraform Module Library
        (3, 15),  // Event-Driven Architecture -> WebSocket Scaling
        (4, 18),  // Database Sharding -> PostgreSQL Replication
        (5, 16),  // CI/CD Pipeline -> Monorepo Build Optimization
        (6, 11),  // K8s Hardening -> gRPC Service Mesh
        (7, 2),   // GraphQL Federation -> API Gateway Design
        (9, 11),  // Observability Stack -> gRPC Service Mesh
        (10, 5),  // Zero-Downtime -> CI/CD Pipeline
        (12, 4),  // Data Lake Architecture -> Database Sharding
        (13, 16), // Frontend Performance -> Monorepo Build
        (14, 1),  // Terraform Module -> AWS Cost Optimization
        (19, 6),  // Container Image Security -> K8s Hardening
        (20, 13), // Q4 Product Roadmap -> Frontend Performance
        (23, 20), // Product-Led Growth -> Q4 Product Roadmap
        (25, 20), // API Monetization -> Q4 Product Roadmap
        (30, 35), // SOC2 Compliance -> Vulnerability Management
        (31, 30), // Incident Response -> SOC2 Compliance
        (33, 6),  // Zero Trust Architecture -> K8s Hardening
        (33, 38), // Zero Trust Architecture -> Secret Management
        (34, 37), // SIEM Configuration -> Cloud Security
        (36, 34), // Cloud Security -> SIEM
        (40, 47), // Remote Work Policy -> Employee Handbook
        (41, 45), // Career Ladder -> New Hire Onboarding
        (50, 51), // GDPR Data Deletion -> CCPA Compliance
        (51, 59), // CCPA -> Privacy Impact Assessment
        (53, 56), // Open Source License -> AI Ethics
        (56, 60), // AI Ethics -> ML Model Governance
        (60, 64), // ML Model Governance -> Recommendation Engine
        (61, 60), // Feature Store -> ML Model Governance
        (62, 61), // A/B Testing -> Feature Store
        (63, 64), // NLP Pipeline -> Recommendation Engine
        (65, 62), // Data Quality -> A/B Testing
        (68, 14), // IaC Standards -> Terraform Module
        (69, 31), // Disaster Recovery -> Incident Response
        (71, 69), // SRE On-Call -> Disaster Recovery
        (73, 1),  // Cost Tagging -> AWS Cost Optimization
        (76, 3),  // Platform ADR Index -> Event-Driven Architecture
        (77, 3),  // Event Mesh -> Event-Driven Architecture
        (78, 7),  // API Versioning -> GraphQL Federation
        (79, 3),  // DDD Reference -> Event-Driven Architecture
        (80, 12), // Multi-Tenancy Architecture -> Data Lake Architecture
        (81, 3),  // CQRS -> Event-Driven Architecture
        (83, 12), // Data Mesh Governance -> Data Lake Architecture
        // New document cross-references (indices 84+)
        (84, 19),  // Memory Safety Audit -> Container Image Security
        (85, 10),  // Load Testing Framework -> Zero-Downtime Deployment
        (86, 10),  // Error Budget Policy -> Zero-Downtime Deployment
        (87, 13),  // Feature Toggle Architecture -> Frontend Performance
        (88, 3),   // Async Message Processing -> Event-Driven Architecture
        (89, 4),   // Database Migration Runbook -> Database Sharding
        (91, 2),   // Rate Limiting Design -> API Gateway Design
        (92, 9),   // SLO Catalog -> Observability Stack
        (93, 5),   // Code Review Standards -> CI/CD Pipeline
        (94, 19),  // Dependency Management -> Container Image Security
        (95, 9),   // Distributed Tracing -> Observability Stack
        (96, 78),  // API Pagination Standards -> API Versioning
        (97, 3),   // State Machine Design -> Event-Driven Architecture
        (99, 13),  // Image Processing Pipeline -> Frontend Performance
        (100, 5),  // Internal Developer Portal -> CI/CD Pipeline
        (101, 15), // Socket.IO Cluster -> WebSocket Scaling
        (103, 84), // Technical Debt Inventory -> Memory Safety Audit
        (104, 7),  // GraphQL Schema Design -> GraphQL Federation
        (105, 10), // Canary Release -> Zero-Downtime Deployment
        (106, 0),  // Rust Service Template -> Microservices Migration
        (107, 3),  // Event Schema Registry -> Event-Driven Architecture
        (108, 18), // Database Connection Pooling -> PostgreSQL Replication
        (109, 13), // Frontend Testing Strategy -> Frontend Performance
        (110, 6),  // Network Segmentation -> K8s Hardening
        (111, 31), // Incident Post-Mortem -> Incident Response Playbook
        (112, 78), // API SDK Generation -> API Versioning
        (113, 12), // Search Indexing Architecture -> Data Lake Architecture
        (114, 20), // Voice of Customer -> Q4 Product Roadmap
        (115, 25), // Pricing Tier -> API Monetization
        (118, 20), // Notification System -> Q4 Product Roadmap
        (120, 13), // Accessibility Report -> Frontend Performance
        (123, 20), // Customer Success Playbook -> Q4 Product Roadmap
        (127, 50), // Compliance Dashboard -> GDPR Data Deletion
        (128, 51), // Customer Data Platform -> CCPA Compliance
        (129, 30), // Bug Bounty -> SOC2 Compliance
        (130, 2),  // API Security Assessment -> API Gateway Design
        (131, 38), // Encryption Standards -> Secret Management
        (132, 33), // IAM Design -> Zero Trust Architecture
        (133, 6),  // Container Runtime Security -> K8s Hardening
        (136, 34), // Network Intrusion Detection -> SIEM Configuration
        (137, 19), // Supply Chain Security -> Container Image Security
        (139, 33), // DDoS Protection -> Zero Trust Architecture
        (140, 47), // Employee Engagement Survey -> Employee Handbook
        (141, 41), // Technical Interview Rubric -> Career Ladder
        (146, 45), // Knowledge Transfer Protocol -> New Hire Onboarding
        (149, 47), // Internal Mobility -> Employee Handbook
        (150, 56), // Anti-Bribery -> AI Ethics
        (152, 50), // Incident Notification -> GDPR Data Deletion
        (153, 51), // Terms of Service -> CCPA Compliance
        (155, 56), // Regulatory Change Impact -> AI Ethics
        (159, 50), // Cross-Border Data Transfer -> GDPR Data Deletion
        (160, 62), // Experiment Platform -> A/B Testing
        (161, 56), // Bias Detection -> AI Ethics
        (162, 12), // Data Catalog -> Data Lake Architecture
        (164, 60), // Customer LTV Model -> ML Model Governance
        (165, 56), // Data Privacy Engineering -> AI Ethics
        (166, 64), // Real-Time Scoring -> Recommendation Engine
        (167, 65), // Anomaly Detection Playbook -> Data Quality
        (170, 60), // Causal Inference -> ML Model Governance
        (171, 63), // LLM Fine-Tuning -> NLP Pipeline
        (172, 68), // Multi-Cloud Strategy -> IaC Standards
        (173, 6),  // K8s Upgrade Runbook -> K8s Hardening
        (174, 68), // GitOps Workflow -> IaC Standards
        (175, 69), // Database Backup -> Disaster Recovery
        (176, 11), // Service Mesh Perf -> gRPC Service Mesh
        (177, 1),  // Capacity Planning -> AWS Cost Optimization
        (178, 31), // Incident Management Tool -> Incident Response
        (180, 9),  // Platform Health Dashboard -> Observability Stack
        (181, 1),  // FinOps Assessment -> AWS Cost Optimization
        (183, 68), // Production Readiness Review -> IaC Standards
        (184, 0),  // Strangler Fig -> Microservices Migration
        (185, 3),  // Saga Pattern -> Event-Driven Architecture
        (186, 2),  // Cache Architecture -> API Gateway Design
        (187, 11), // Sidecar Pattern -> gRPC Service Mesh
        (188, 81), // Event Sourcing Ref -> CQRS
        (189, 0),  // Bulkhead Pattern -> Microservices Migration
        (190, 7),  // API Composition -> GraphQL Federation
        (191, 9),  // Observability Ref Arch -> Observability Stack
        (192, 12), // Data Replication -> Data Lake Architecture
        (193, 33), // Security Architecture Review -> Zero Trust
        (194, 12), // Polyglot Persistence -> Data Lake Architecture
    ];

    for (src_idx, tgt_idx) in &reference_pairs {
        let src = samyama::graph::NodeId::new(doc_ids[*src_idx]);
        let tgt = samyama::graph::NodeId::new(doc_ids[*tgt_idx]);
        store.create_edge(src, tgt, "REFERENCES").unwrap();
        edge_count += 1;
    }

    // DEPENDS_ON: Project -> Project
    let project_deps: Vec<(usize, usize)> = vec![
        (0, 4),   // Atlas Platform -> Horizon Mobile App
        (1, 0),   // Phoenix Migration -> Atlas Platform
        (2, 6),   // Quantum ML -> Nebula Analytics
        (3, 13),  // Sentinel -> Shield Zero Trust
        (4, 9),   // Horizon -> Apex API
        (5, 11),  // Titan Infrastructure -> Forge CI/CD
        (6, 23),  // Nebula Analytics -> Delta Data Lake
        (7, 3),   // Guardian Compliance -> Sentinel Security
        (9, 14),  // Apex API -> Echo Event Platform
        (10, 14), // Catalyst Data Mesh -> Echo Event Platform
        (12, 5),  // Prism Observability -> Titan Infrastructure
        (14, 0),  // Echo Event -> Atlas Platform
        (16, 9),  // Bridge Integration -> Apex API
        (17, 12), // Lighthouse -> Prism Observability
        (18, 5),  // Orbit Multi-Region -> Titan Infrastructure
        (19, 0),  // Vertex Graph -> Atlas Platform
        (20, 2),  // Aurora AI -> Quantum ML
        (21, 13), // Bastion -> Shield Zero Trust
        (24, 22), // Ember Feature Flags -> Crystal Reports
        (25, 3),  // Falcon Threat Detection -> Sentinel
        (27, 19), // Helix Search -> Vertex Graph
        (28, 8),  // Iron Cache -> Pulse (should be related)
    ];

    for (src_idx, tgt_idx) in &project_deps {
        let src = samyama::graph::NodeId::new(proj_ids[*src_idx]);
        let tgt = samyama::graph::NodeId::new(proj_ids[*tgt_idx]);
        store.create_edge(src, tgt, "DEPENDS_ON").unwrap();
        edge_count += 1;
    }

    // WORKS_ON: Employee -> Project
    let works_on: Vec<(usize, usize)> = vec![
        (0, 0),   // Sarah Chen -> Atlas Platform
        (0, 9),   // Sarah Chen -> Apex API
        (1, 5),   // Marcus Johnson -> Titan Infrastructure
        (2, 14),  // James Liu -> Echo Event Platform
        (2, 10),  // James Liu -> Catalyst Data Mesh
        (3, 23),  // Priya Sharma -> Delta Data Lake
        (4, 11),  // Derek Williams -> Forge CI/CD
        (5, 13),  // Alex Rivera -> Shield Zero Trust
        (6, 4),   // Emily Tran -> Horizon Mobile App
        (7, 0),   // Kevin Park -> Atlas Platform
        (8, 12),  // Natasha Volkov -> Prism Observability
        (9, 18),  // Omar Hassan -> Orbit Multi-Region
        (10, 4),  // Lisa Park -> Horizon Mobile App
        (11, 22), // Michael Torres -> Crystal Reports
        (12, 15), // Rachel Green -> Compass Navigation
        (15, 3),  // David Kim -> Sentinel Security Suite
        (16, 13), // Jennifer Walsh -> Shield Zero Trust
        (17, 25), // Ryan Foster -> Falcon Threat Detection
        (20, 8),  // Angela Foster -> Pulse Employee Experience
        (21, 26), // Tom Bradley -> Genesis Onboarding
        (23, 7),  // Robert Nguyen -> Guardian Compliance
        (24, 7),  // Patricia Hernandez -> Guardian Compliance
        (26, 2),  // Wei Zhang -> Quantum ML Pipeline
        (27, 20), // Aisha Okafor -> Aurora AI Features
        (28, 6),  // Chris Taylor -> Nebula Analytics
        (29, 23), // Lauren Mitchell -> Delta Data Lake
        (30, 5),  // Carlos Mendez -> Titan Infrastructure
        (31, 17), // Nina Petrov -> Lighthouse Monitoring
        (32, 18), // Jason Lee -> Orbit Multi-Region
        (33, 11), // Amanda Ross -> Forge CI/CD
        (34, 16), // Benjamin Cole -> Bridge Integration Hub
        (35, 18), // Stephanie Wright -> Orbit Multi-Region
        (36, 10), // Nathan Brooks -> Catalyst Data Mesh
        (37, 29), // Diana Lopez -> Jade Documentation
        (38, 0),  // Samuel Osei -> Atlas Platform
        (39, 0),  // Victoria Patel -> Atlas Platform
        (40, 0),  // Thomas Wright -> Atlas Platform (CTO oversight)
        (26, 20), // Wei Zhang -> Aurora AI Features
        (27, 2),  // Aisha Okafor -> Quantum ML Pipeline
        (30, 11), // Carlos Mendez -> Forge CI/CD
    ];

    for (emp_idx, proj_idx) in &works_on {
        let emp = samyama::graph::NodeId::new(emp_ids[*emp_idx]);
        let proj = samyama::graph::NodeId::new(proj_ids[*proj_idx]);
        store.create_edge(emp, proj, "WORKS_ON").unwrap();
        edge_count += 1;
    }

    // MANAGES: Employee -> Project (management relationships)
    let manages: Vec<(usize, usize)> = vec![
        (0, 0),   // Sarah Chen manages Atlas Platform
        (4, 11),  // Derek Williams manages Forge CI/CD
        (10, 4),  // Lisa Park manages Horizon Mobile App
        (15, 3),  // David Kim manages Sentinel Security Suite
        (20, 8),  // Angela Foster manages Pulse Employee Experience
        (23, 7),  // Robert Nguyen manages Guardian Compliance
        (26, 2),  // Wei Zhang manages Quantum ML Pipeline
        (30, 5),  // Carlos Mendez manages Titan Infrastructure
        (39, 19), // Victoria Patel manages Vertex Graph Platform
        (40, 0),  // Thomas Wright manages Atlas Platform (executive sponsor)
    ];

    for (emp_idx, proj_idx) in &manages {
        let emp = samyama::graph::NodeId::new(emp_ids[*emp_idx]);
        let proj = samyama::graph::NodeId::new(proj_ids[*proj_idx]);
        store.create_edge(emp, proj, "MANAGES").unwrap();
        edge_count += 1;
    }

    // USES_TECH: Project -> Technology
    let uses_tech: Vec<(usize, usize)> = vec![
        (0, 0),   // Atlas Platform -> Kubernetes
        (0, 1),   // Atlas Platform -> PostgreSQL
        (0, 2),   // Atlas Platform -> Redis
        (0, 3),   // Atlas Platform -> Kafka
        (0, 6),   // Atlas Platform -> gRPC
        (1, 0),   // Phoenix Migration -> Kubernetes
        (1, 7),   // Phoenix Migration -> Docker
        (2, 16),  // Quantum ML -> MLflow
        (2, 15),  // Quantum ML -> Spark
        (2, 22),  // Quantum ML -> Feast
        (3, 18),  // Sentinel -> Splunk
        (3, 23),  // Sentinel -> Trivy
        (4, 5),   // Horizon -> React
        (5, 4),   // Titan Infrastructure -> Terraform
        (5, 0),   // Titan Infrastructure -> Kubernetes
        (5, 7),   // Titan Infrastructure -> Docker
        (6, 15),  // Nebula Analytics -> Spark
        (6, 14),  // Nebula Analytics -> Delta Lake
        (6, 13),  // Nebula Analytics -> S3
        (9, 11),  // Apex API -> GraphQL
        (9, 6),   // Apex API -> gRPC
        (9, 2),   // Apex API -> Redis
        (10, 3),  // Catalyst Data Mesh -> Kafka
        (10, 13), // Catalyst Data Mesh -> S3
        (11, 7),  // Forge CI/CD -> Docker
        (11, 4),  // Forge CI/CD -> Terraform
        (12, 8),  // Prism Observability -> Prometheus
        (12, 9),  // Prism Observability -> Grafana
        (12, 20), // Prism Observability -> OpenTelemetry
        (13, 10), // Shield Zero Trust -> Istio
        (13, 17), // Shield Zero Trust -> Vault
        (14, 3),  // Echo Event Platform -> Kafka
        (14, 2),  // Echo Event Platform -> Redis
        (17, 8),  // Lighthouse -> Prometheus
        (17, 9),  // Lighthouse -> Grafana
        (18, 0),  // Orbit Multi-Region -> Kubernetes
        (18, 4),  // Orbit Multi-Region -> Terraform
        (19, 1),  // Vertex Graph -> PostgreSQL
        (19, 2),  // Vertex Graph -> Redis
        (20, 16), // Aurora AI -> MLflow
        (20, 15), // Aurora AI -> Spark
        (21, 17), // Bastion -> Vault
        (21, 21), // Bastion -> Consul
        (23, 13), // Delta Data Lake -> S3
        (23, 14), // Delta Data Lake -> Delta Lake
        (25, 18), // Falcon Threat Detection -> Splunk
        (27, 12), // Helix Search -> Elasticsearch
        (28, 2),  // Iron Cache -> Redis
    ];

    for (proj_idx, tech_idx) in &uses_tech {
        let proj = samyama::graph::NodeId::new(proj_ids[*proj_idx]);
        let tech = samyama::graph::NodeId::new(tech_ids[*tech_idx]);
        store.create_edge(proj, tech, "USES_TECH").unwrap();
        edge_count += 1;
    }

    // EXPERT_IN: Employee -> Technology
    let expert_in: Vec<(usize, usize)> = vec![
        (0, 0),   // Sarah Chen -> Kubernetes
        (0, 6),   // Sarah Chen -> gRPC
        (0, 11),  // Sarah Chen -> GraphQL
        (1, 8),   // Marcus Johnson -> Prometheus
        (1, 7),   // Marcus Johnson -> Docker
        (2, 3),   // James Liu -> Kafka
        (2, 2),   // James Liu -> Redis
        (3, 1),   // Priya Sharma -> PostgreSQL
        (3, 14),  // Priya Sharma -> Delta Lake
        (4, 4),   // Derek Williams -> Terraform
        (4, 7),   // Derek Williams -> Docker
        (5, 10),  // Alex Rivera -> Istio
        (5, 17),  // Alex Rivera -> Vault
        (5, 21),  // Alex Rivera -> Consul
        (6, 5),   // Emily Tran -> React
        (15, 18), // David Kim -> Splunk
        (16, 18), // Jennifer Walsh -> Splunk
        (16, 23), // Jennifer Walsh -> Trivy
        (26, 16), // Wei Zhang -> MLflow
        (26, 15), // Wei Zhang -> Spark
        (27, 22), // Aisha Okafor -> Feast
        (27, 16), // Aisha Okafor -> MLflow
        (30, 4),  // Carlos Mendez -> Terraform
        (30, 0),  // Carlos Mendez -> Kubernetes
        (31, 8),  // Nina Petrov -> Prometheus
        (31, 9),  // Nina Petrov -> Grafana
        (32, 0),  // Jason Lee -> Kubernetes
        (32, 4),  // Jason Lee -> Terraform
    ];

    for (emp_idx, tech_idx) in &expert_in {
        let emp = samyama::graph::NodeId::new(emp_ids[*emp_idx]);
        let tech = samyama::graph::NodeId::new(tech_ids[*tech_idx]);
        store.create_edge(emp, tech, "EXPERT_IN").unwrap();
        edge_count += 1;
    }

    println!("  Created {} relationships (AUTHORED, REFERENCES, DEPENDS_ON,", edge_count);
    println!("    WORKS_ON, MANAGES, USES_TECH, EXPERT_IN)");
    println!();
    println!("  Knowledge Graph Summary:");
    println!("  ┌──────────────────────┬───────┐");
    println!("  │ Entity Type          │ Count │");
    println!("  ├──────────────────────┼───────┤");
    println!("  │ Documents            │  {:>4} │", documents.len());
    println!("  │ Employees            │  {:>4} │", employees.len());
    println!("  │ Projects             │  {:>4} │", projects.len());
    println!("  │ Technologies         │  {:>4} │", technologies.len());
    println!("  │ Relationships        │  {:>4} │", edge_count);
    println!("  └──────────────────────┴───────┘");
    println!();

    // ════════════════════════════════════════════════════════════════════════════════════════
    // Step 2: Semantic Document Search
    // ════════════════════════════════════════════════════════════════════════════════════════
    println!("Step 2: Semantic Document Search");
    separator();
    println!("  Query: \"Find documents about GDPR compliance and data privacy\"");
    println!();

    // Use a seed close to the GDPR/privacy docs (indices 50-59 in legal area)
    let query_vec = query_embedding(52);
    let search_results = store
        .vector_search("Document", "embedding", &query_vec, 8)
        .unwrap();

    println!("  ┌────┬────────────────────────────────────────────────────────┬────────────┬──────────┐");
    println!("  │ #  │ Document Title                                         │ Department │ Score    │");
    println!("  ├────┼────────────────────────────────────────────────────────┼────────────┼──────────┤");
    for (rank, (nid, score)) in search_results.iter().enumerate() {
        let node = store.get_node(*nid).unwrap();
        let title = node.get_property("title").unwrap().as_string().unwrap();
        let dept = node.get_property("department").unwrap().as_string().unwrap();
        let truncated = if title.len() > 54 {
            format!("{}...", &title[..51])
        } else {
            title.to_string()
        };
        println!(
            "  │ {:>2} │ {:<54} │ {:<10} │ {:>8.4} │",
            rank + 1,
            truncated,
            dept,
            1.0 - score
        );
    }
    println!("  └────┴────────────────────────────────────────────────────────┴────────────┴──────────┘");
    println!();

    // ════════════════════════════════════════════════════════════════════════════════════════
    // Step 3: Expert Finding (Vector Search + Graph Traversal)
    // ════════════════════════════════════════════════════════════════════════════════════════
    println!("Step 3: Expert Finding -- Who knows about Kubernetes security?");
    separator();
    println!("  Strategy: Vector search for relevant docs, then traverse AUTHORED");
    println!("  edges to find authors, then check EXPERT_IN for technology skills.");
    println!();

    // Search for Kubernetes-related docs (use a seed close to K8s docs at index 6)
    let k8s_query = query_embedding(6);
    let k8s_results = store
        .vector_search("Document", "embedding", &k8s_query, 5)
        .unwrap();

    // Collect authors of top matching documents
    let mut expert_scores: HashMap<String, (f64, Vec<String>)> = HashMap::new();

    for (nid, score) in &k8s_results {
        let doc_node = store.get_node(*nid).unwrap();
        let doc_title = doc_node.get_property("title").unwrap().as_string().unwrap();
        let relevance = (1.0 - *score) as f64;

        // Find who authored this doc: traverse incoming AUTHORED edges
        let incoming = store.get_incoming_edges(*nid);
        for edge in &incoming {
            if edge.edge_type == samyama::graph::EdgeType::new("AUTHORED") {
                if let Some(author_node) = store.get_node(edge.source) {
                    let author_name = author_node
                        .get_property("name")
                        .unwrap()
                        .as_string()
                        .unwrap()
                        .to_string();
                    let entry = expert_scores
                        .entry(author_name.clone())
                        .or_insert((0.0, Vec::new()));
                    entry.0 += relevance;
                    entry.1.push(doc_title.to_string());
                }
            }
        }
    }

    // Sort experts by accumulated relevance score
    let mut experts: Vec<(String, f64, Vec<String>)> = expert_scores
        .into_iter()
        .map(|(name, (score, docs))| (name, score, docs))
        .collect();
    experts.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());

    println!("  Top Experts (by document relevance):");
    println!("  ┌────┬──────────────────────┬──────────────┬──────────────────────────────────────────┐");
    println!("  │ #  │ Expert               │ Relevance    │ Key Documents                            │");
    println!("  ├────┼──────────────────────┼──────────────┼──────────────────────────────────────────┤");
    for (i, (name, score, docs)) in experts.iter().take(5).enumerate() {
        let doc_summary = if docs.len() == 1 {
            let d = &docs[0];
            if d.len() > 38 { format!("{}...", &d[..35]) } else { d.clone() }
        } else {
            format!("{} documents authored", docs.len())
        };
        println!(
            "  │ {:>2} │ {:<20} │ {:>12.4} │ {:<40} │",
            i + 1,
            name,
            score,
            doc_summary
        );
    }
    println!("  └────┴──────────────────────┴──────────────┴──────────────────────────────────────────┘");

    // Check technology expertise for top expert
    if let Some((top_expert, _, _)) = experts.first() {
        if let Some(&emp_raw) = name_to_emp_id.get(top_expert) {
            let emp_nid = samyama::graph::NodeId::new(emp_raw);
            let outgoing = store.get_outgoing_edges(emp_nid);
            let tech_skills: Vec<String> = outgoing
                .iter()
                .filter(|e| e.edge_type == samyama::graph::EdgeType::new("EXPERT_IN"))
                .filter_map(|e| {
                    store
                        .get_node(e.target)
                        .and_then(|n| n.get_property("name"))
                        .and_then(|p| p.as_string())
                        .map(|s| s.to_string())
                })
                .collect();
            if !tech_skills.is_empty() {
                println!();
                println!(
                    "  {} is also an expert in: {}",
                    top_expert,
                    tech_skills.join(", ")
                );
            }
        }
    }
    println!();

    // ════════════════════════════════════════════════════════════════════════════════════════
    // Step 4: Document Lineage -- Trace References and Dependencies
    // ════════════════════════════════════════════════════════════════════════════════════════
    println!("Step 4: Document Lineage -- Tracing References");
    separator();

    // Pick "Event-Driven Architecture Blueprint" (index 3) as the root document
    let root_doc_idx = 3;
    let root_doc_nid = samyama::graph::NodeId::new(doc_ids[root_doc_idx]);
    let root_title = documents[root_doc_idx].0;

    println!("  Root Document: \"{}\"", root_title);
    println!();

    // Find documents that REFERENCE this document (incoming REFERENCES edges)
    let incoming_refs = store.get_incoming_edges(root_doc_nid);
    let referencing_docs: Vec<(&str, &str)> = incoming_refs
        .iter()
        .filter(|e| e.edge_type == samyama::graph::EdgeType::new("REFERENCES"))
        .filter_map(|e| {
            store.get_node(e.source).map(|n| {
                let t = n.get_property("title").unwrap().as_string().unwrap();
                let d = n.get_property("department").unwrap().as_string().unwrap();
                (t, d)
            })
        })
        .collect();

    println!("  Documents that REFERENCE this document ({} found):", referencing_docs.len());
    println!("  ┌────┬──────────────────────────────────────────────────────────┬──────────────┐");
    println!("  │ #  │ Referencing Document                                     │ Department   │");
    println!("  ├────┼──────────────────────────────────────────────────────────┼──────────────┤");
    for (i, (title, dept)) in referencing_docs.iter().enumerate() {
        let truncated = if title.len() > 56 {
            format!("{}...", &title[..53])
        } else {
            title.to_string()
        };
        println!("  │ {:>2} │ {:<56} │ {:<12} │", i + 1, truncated, dept);
    }
    println!("  └────┴──────────────────────────────────────────────────────────┴──────────────┘");

    // Find documents this document REFERENCES (outgoing REFERENCES edges)
    let outgoing_refs = store.get_outgoing_edges(root_doc_nid);
    let referenced_docs: Vec<(&str, &str)> = outgoing_refs
        .iter()
        .filter(|e| e.edge_type == samyama::graph::EdgeType::new("REFERENCES"))
        .filter_map(|e| {
            store.get_node(e.target).map(|n| {
                let t = n.get_property("title").unwrap().as_string().unwrap();
                let d = n.get_property("department").unwrap().as_string().unwrap();
                (t, d)
            })
        })
        .collect();

    println!();
    println!("  Documents this document REFERENCES ({} found):", referenced_docs.len());
    println!("  ┌────┬──────────────────────────────────────────────────────────┬──────────────┐");
    println!("  │ #  │ Referenced Document                                      │ Department   │");
    println!("  ├────┼──────────────────────────────────────────────────────────┼──────────────┤");
    for (i, (title, dept)) in referenced_docs.iter().enumerate() {
        let truncated = if title.len() > 56 {
            format!("{}...", &title[..53])
        } else {
            title.to_string()
        };
        println!("  │ {:>2} │ {:<56} │ {:<12} │", i + 1, truncated, dept);
    }
    println!("  └────┴──────────────────────────────────────────────────────────┴──────────────┘");
    println!();

    // Cross-domain insight: use Cypher to find engineering docs referenced by legal docs
    println!("  Cross-Domain Insight: Engineering docs referenced by Legal documents");
    let cross_domain_query =
        "MATCH (legal:Document)-[:REFERENCES]->(eng:Document) WHERE legal.department = 'Legal' AND eng.department = 'Data Science' RETURN legal.title, eng.title";
    // Note: The Cypher engine may not support all WHERE clause patterns for property
    // comparisons. We use direct graph traversal for reliable cross-domain discovery.
    let _ = engine.execute(cross_domain_query, &store); // Best-effort Cypher attempt

    // Manual cross-domain discovery
    let legal_docs: Vec<_> = store
        .get_nodes_by_label(&Label::new("Document"))
        .into_iter()
        .filter(|n| {
            n.get_property("department")
                .and_then(|p| p.as_string())
                .map(|s| s == "Legal")
                .unwrap_or(false)
        })
        .collect();

    let mut cross_domain_links: Vec<(String, String, String)> = Vec::new();
    for legal_node in &legal_docs {
        let outgoing = store.get_outgoing_edges(legal_node.id);
        for edge in &outgoing {
            if edge.edge_type == samyama::graph::EdgeType::new("REFERENCES") {
                if let Some(target) = store.get_node(edge.target) {
                    let target_dept = target
                        .get_property("department")
                        .and_then(|p| p.as_string())
                        .unwrap_or("Unknown");
                    if target_dept != "Legal" {
                        let legal_title = legal_node
                            .get_property("title")
                            .unwrap()
                            .as_string()
                            .unwrap();
                        let target_title =
                            target.get_property("title").unwrap().as_string().unwrap();
                        cross_domain_links.push((
                            legal_title.to_string(),
                            target_title.to_string(),
                            target_dept.to_string(),
                        ));
                    }
                }
            }
        }
    }

    if !cross_domain_links.is_empty() {
        println!("  ┌──────────────────────────────────────┬──────────────────────────────────────┬──────────────┐");
        println!("  │ Legal Document                       │ References                           │ Target Dept  │");
        println!("  ├──────────────────────────────────────┼──────────────────────────────────────┼──────────────┤");
        for (legal, target, dept) in &cross_domain_links {
            let l = if legal.len() > 36 {
                format!("{}...", &legal[..33])
            } else {
                legal.clone()
            };
            let t = if target.len() > 36 {
                format!("{}...", &target[..33])
            } else {
                target.clone()
            };
            println!("  │ {:<36} │ {:<36} │ {:<12} │", l, t, dept);
        }
        println!("  └──────────────────────────────────────┴──────────────────────────────────────┴──────────────┘");
    }
    println!();

    // ════════════════════════════════════════════════════════════════════════════════════════
    // Step 5: Topic Clustering using Weakly Connected Components
    // ════════════════════════════════════════════════════════════════════════════════════════
    println!("Step 5: Topic Clustering (Weakly Connected Components)");
    separator();
    println!("  Analyzing document communities based on REFERENCES relationships...");
    println!();

    // Build a view of just Document nodes with REFERENCES edges
    let doc_view = build_view(&store, Some("Document"), Some("REFERENCES"), None);
    let wcc_result = weakly_connected_components(&doc_view);

    println!(
        "  Found {} document communities (connected components)",
        wcc_result.components.len()
    );
    println!();

    // Group documents by component
    let mut component_docs: HashMap<usize, Vec<(String, String)>> = HashMap::new();
    for (&node_id, &comp_id) in &wcc_result.node_component {
        // node_id is already the original NodeId (u64), not an index
        let nid = samyama::graph::NodeId::new(node_id);
        if let Some(node) = store.get_node(nid) {
            let title = node
                .get_property("title")
                .and_then(|p| p.as_string())
                .unwrap_or("Unknown")
                .to_string();
            let dept = node
                .get_property("department")
                .and_then(|p| p.as_string())
                .unwrap_or("Unknown")
                .to_string();
            component_docs
                .entry(comp_id)
                .or_default()
                .push((title, dept));
        }
    }

    // Sort components by size (largest first)
    let mut sorted_components: Vec<(usize, Vec<(String, String)>)> =
        component_docs.into_iter().collect();
    sorted_components.sort_by(|a, b| b.1.len().cmp(&a.1.len()));

    // Display top communities
    let display_count = sorted_components.len().min(5);
    for (rank, (comp_id, docs)) in sorted_components.iter().take(display_count).enumerate() {
        // Determine dominant department
        let mut dept_counts: HashMap<&str, usize> = HashMap::new();
        for (_, dept) in docs {
            *dept_counts.entry(dept.as_str()).or_default() += 1;
        }
        let mut dept_sorted: Vec<_> = dept_counts.into_iter().collect();
        dept_sorted.sort_by(|a, b| b.1.cmp(&a.1));
        let dominant_depts: Vec<String> = dept_sorted
            .iter()
            .take(3)
            .map(|(d, c)| format!("{} ({})", d, c))
            .collect();

        println!(
            "  Community {} (ID: {}, {} documents)",
            rank + 1,
            comp_id,
            docs.len()
        );
        println!("    Departments: {}", dominant_depts.join(", "));
        println!("    Sample documents:");
        for (title, dept) in docs.iter().take(4) {
            let truncated = if title.len() > 52 {
                format!("{}...", &title[..49])
            } else {
                title.clone()
            };
            println!("      - {} [{}]", truncated, dept);
        }
        if docs.len() > 4 {
            println!("      ... and {} more", docs.len() - 4);
        }
        println!();
    }

    // ════════════════════════════════════════════════════════════════════════════════════════
    // Step 6: Knowledge Hub Identification using PageRank
    // ════════════════════════════════════════════════════════════════════════════════════════
    println!("Step 6: Knowledge Hub Identification (PageRank)");
    separator();
    println!("  Finding the most referenced and influential documents...");
    println!();

    let full_doc_view = build_view(&store, Some("Document"), Some("REFERENCES"), None);
    let pr_scores = page_rank(&full_doc_view, PageRankConfig::default());

    // Map PageRank scores back to document info
    let mut doc_pr: Vec<(String, String, f64)> = pr_scores
        .iter()
        .map(|(&node_id, &score)| {
            // node_id is already the original NodeId (u64)
            let nid = samyama::graph::NodeId::new(node_id);
            let node = store.get_node(nid).unwrap();
            let title = node
                .get_property("title")
                .and_then(|p| p.as_string())
                .unwrap_or("Unknown")
                .to_string();
            let dept = node
                .get_property("department")
                .and_then(|p| p.as_string())
                .unwrap_or("Unknown")
                .to_string();
            (title, dept, score)
        })
        .collect();

    doc_pr.sort_by(|a, b| b.2.partial_cmp(&a.2).unwrap());

    println!("  Top 15 Knowledge Hubs (by PageRank score):");
    println!("  ┌────┬──────────────────────────────────────────────────────────┬──────────────┬───────────┐");
    println!("  │ #  │ Document Title                                           │ Department   │ PageRank  │");
    println!("  ├────┼──────────────────────────────────────────────────────────┼──────────────┼───────────┤");
    for (i, (title, dept, score)) in doc_pr.iter().take(15).enumerate() {
        let truncated = if title.len() > 56 {
            format!("{}...", &title[..53])
        } else {
            title.clone()
        };
        println!(
            "  │ {:>2} │ {:<56} │ {:<12} │ {:>9.6} │",
            i + 1,
            truncated,
            dept,
            score
        );
    }
    println!("  └────┴──────────────────────────────────────────────────────────┴──────────────┴───────────┘");
    println!();

    // Identify the top knowledge hub and show its influence
    if let Some((hub_title, hub_dept, hub_score)) = doc_pr.first() {
        println!("  Top Knowledge Hub: \"{}\"", hub_title);
        println!("    Department: {}, PageRank: {:.6}", hub_dept, hub_score);

        // Find the corresponding NodeId
        let hub_nid_opt = store
            .get_nodes_by_label(&Label::new("Document"))
            .into_iter()
            .find(|n| {
                n.get_property("title")
                    .and_then(|p| p.as_string())
                    .map(|s| s == hub_title.as_str())
                    .unwrap_or(false)
            });

        if let Some(hub_node) = hub_nid_opt {
            let incoming = store.get_incoming_edges(hub_node.id);
            let ref_count = incoming
                .iter()
                .filter(|e| e.edge_type == samyama::graph::EdgeType::new("REFERENCES"))
                .count();
            println!("    Referenced by: {} other documents", ref_count);
        }
    }
    println!();

    // Also run PageRank on projects to find critical infrastructure
    println!("  Project Dependency Analysis (PageRank on DEPENDS_ON):");
    let proj_view = build_view(&store, Some("Project"), Some("DEPENDS_ON"), None);
    let proj_pr = page_rank(&proj_view, PageRankConfig::default());

    let mut proj_pr_sorted: Vec<(String, f64)> = proj_pr
        .iter()
        .map(|(&node_id, &score)| {
            // node_id is already the original NodeId (u64)
            let nid = samyama::graph::NodeId::new(node_id);
            let node = store.get_node(nid).unwrap();
            let name = node
                .get_property("name")
                .and_then(|p| p.as_string())
                .unwrap_or("Unknown")
                .to_string();
            (name, score)
        })
        .collect();
    proj_pr_sorted.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());

    println!("  ┌────┬──────────────────────────────────┬───────────┐");
    println!("  │ #  │ Project                          │ PageRank  │");
    println!("  ├────┼──────────────────────────────────┼───────────┤");
    for (i, (name, score)) in proj_pr_sorted.iter().take(10).enumerate() {
        println!(
            "  │ {:>2} │ {:<32} │ {:>9.6} │",
            i + 1,
            name,
            score
        );
    }
    println!("  └────┴──────────────────────────────────┴───────────┘");
    println!();

    // ════════════════════════════════════════════════════════════════════════════════════════
    // Summary
    // ════════════════════════════════════════════════════════════════════════════════════════
    let total_nodes =
        documents.len() + employees.len() + projects.len() + technologies.len();

    separator();
    println!("  Enterprise Knowledge Graph -- Summary");
    separator();
    println!();
    println!("  Graph Statistics:");
    println!(
        "    Nodes: {} documents + {} employees + {} projects + {} technologies = {} total",
        documents.len(),
        employees.len(),
        projects.len(),
        technologies.len(),
        total_nodes
    );
    println!(
        "    Edges: {} relationships across 7 types",
        edge_count
    );
    println!();
    println!("  Capabilities Demonstrated:");
    println!("    1. Property graph model with rich metadata");
    println!("    2. 128-dimensional vector search for semantic document discovery");
    println!("    3. Expert finding via vector search + graph traversal");
    println!("    4. Document lineage tracing across departments");
    println!("    5. Topic clustering with Weakly Connected Components");
    println!("    6. Knowledge hub identification with PageRank");
    println!();
    println!("  All data is deterministic. No API keys required.");
    separator();
}
