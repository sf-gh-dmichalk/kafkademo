/* =============================================================================
   00_account_setup.sql — roles and privileges
   -----------------------------------------------------------------------------
   Run as ACCOUNTADMIN.

   Two roles only:
     OF_KAFKA_ADMIN         owns everything (database, warehouses,
                            deployment, runtime, tasks, tables)
     OF_KAFKA_RUNTIME_ROLE  execute-as identity for the Openflow runtime
                            (Openflow requires this to be separate)

   Everything is namespaced to OF_KAFKA_* so teardown never touches
   another demo's objects.
   ============================================================================= */

USE ROLE ACCOUNTADMIN;

/* --- Roles --- */
CREATE ROLE IF NOT EXISTS OF_KAFKA_ADMIN
    COMMENT = 'Owns all objects for the Kafka Openflow demo';

CREATE ROLE IF NOT EXISTS OF_KAFKA_RUNTIME_ROLE
    COMMENT = 'Execute-as role for the Openflow runtime';

/* --- Hierarchy: both under SYSADMIN, both granted to DMICHALK --- */
USE ROLE SECURITYADMIN;
GRANT ROLE OF_KAFKA_ADMIN        TO ROLE SYSADMIN;
GRANT ROLE OF_KAFKA_RUNTIME_ROLE TO ROLE SYSADMIN;
GRANT ROLE OF_KAFKA_ADMIN        TO USER DMICHALK;
GRANT ROLE OF_KAFKA_RUNTIME_ROLE TO USER DMICHALK;

/* --- Account-level privileges for the admin role --- */
USE ROLE ACCOUNTADMIN;

-- Openflow (deployment is on OPENFLOW_ADMIN — shared across demos)
GRANT CREATE COMPUTE POOL ON ACCOUNT TO ROLE OF_KAFKA_ADMIN;

-- Integrations, tasks, alerts
GRANT CREATE INTEGRATION ON ACCOUNT TO ROLE OF_KAFKA_ADMIN;
GRANT EXECUTE TASK       ON ACCOUNT TO ROLE OF_KAFKA_ADMIN;
GRANT EXECUTE ALERT      ON ACCOUNT TO ROLE OF_KAFKA_ADMIN;

-- Cortex AI
GRANT DATABASE ROLE SNOWFLAKE.CORTEX_USER TO ROLE OF_KAFKA_ADMIN;

-- Monitoring
GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO ROLE OF_KAFKA_ADMIN;
GRANT MONITOR USAGE ON ACCOUNT TO ROLE OF_KAFKA_ADMIN;

/* --- Verify --- */
SHOW GRANTS TO ROLE OF_KAFKA_ADMIN;
SHOW GRANTS OF ROLE OF_KAFKA_ADMIN;
