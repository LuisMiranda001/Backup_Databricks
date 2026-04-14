# Databricks Notebook Backup Automation

This project automates the backup of Databricks notebooks across multiple
workspaces using Python and Databricks REST APIs.

## Key Features
- Supports multiple Databricks workspaces
- Centralized notebook export
- Automated and repeatable backup process

## Tech Stack
- Python
- Databricks REST APIs

## Use Case
Designed to support governance, disaster recovery, and operational
automation in data engineering environments.

## Configuration

This project uses environment variables to manage sensitive credentials.

Copy `.env.example` to `.env` and define the required values locally.
The `.env` file is intentionally ignored by Git to prevent credential leaks.
