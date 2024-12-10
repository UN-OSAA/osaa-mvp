```mermaid
flowchart TB
    %% Amazon RDS Resources
    subgraph RDS["Amazon RDS"]
        direction LR
        subgraph PA["Persisted Artifacts"]
            direction LR
            PADB[(PostgreSQL State DB)]
        end
    end

    %% Local Transform Pipeline
    subgraph "Local Transform Pipeline"
        direction LR
        subgraph DTP["SQLMesh"]
            direction TB
            DDB[(DuckDB Database)]
        end
        subgraph LI["Local Ingest (Optional)"]
            direction TB
            LDS[datasources]:::datasource
            LPIP[Ingest]
            LDS ---> LPIP
        end
    end


    %% Github Actions and Deployments
    subgraph "Github"
        direction TB
        AD[Action: Deploy]
        AR[Action: Run]
        AI[Action: Ingest]
        GHCRDB[(Container Registry)]

        AD ---> |Push Image| GHCRDB
        GHCRDB --> |Pull Image| AR
        GHCRDB ----> |Pull Image| AI

        %% SQLMesh Container
        subgraph SQLM["SQLMesh Container"]
                direction TB
                subgraph cicd_check
                    direction TB
                    ITP[Int venv]
                end
                subgraph run_etl_transform
                    direction TB
                    PTP[Prod venv]
                end
            end

            %% Ingest Container
            subgraph ETLI["Ingest Container"]
                direction TB
                DS[datasources]:::datasource
                PIP[Ingest]
                DS --> PIP
            end

    end


    %% S3 Bucket Structure
    subgraph "S3 Bucket (osaa_mvp)"
        direction LR
        %% Datalake Folder
        subgraph DLK[datalake/]
            direction LR
            PL3[landing/]
            PT3[transformed/]
        end

        %% Development Folder
        subgraph DB[username_dev/]
            direction LR
            DT3[transformed/]
        end
    end

    %% Inter-Subgraph Connections
    PL3 -- "Read" --> DTP
    DTP -- "Write" --> DT3
    AR -- "on: push" --> cicd_check
    AR -- "on: schedule" --> run_etl_transform
    AI -- "on: schedule" --> ETLI
    PIP -- "Write" --> PL3
    PA -.-> SQLM
    PL3 -- "Read" --> SQLM
    SQLM -- "Write" --> PT3
    LPIP -. "Write (Optional)" .-> DDB

    %% Styling Definitions
    classDef s3_folder fill:#ff9900,stroke:#232f3e,stroke-width:2px,color:#2d2d2d;
    classDef s3_bucket fill:#ff9900,stroke:#232f3e,stroke-width:2px,color:#f5f5f5,fill-opacity:0.07,stroke-dasharray:5 5;
    classDef process fill:#81b1d9,stroke:#2d5986,stroke-width:2px,color:#2d2d2d;
    classDef multi_process fill:#81b1d9,stroke:#2d5986,stroke-width:3px,color:#2d2d2d,fill-opacity:0.24;
    classDef storage fill:#d4d4d4,stroke:#666666,stroke-width:2px,color:#2d2d2d;
    classDef action fill:#95c37d,stroke:#5a9c3c,stroke-width:2px,color:#2d2d2d;
    classDef registry fill:#f7b93e,stroke:#c78500,stroke-width:2px,color:#2d2d2d;
    classDef trigger fill:#ff6b6b,stroke:#c41e3a,stroke-width:2px,color:#f5f5f5;
    classDef datasource fill:#d4d4d4,stroke:#666666,stroke-width:2px,color:#2d2d2d,stroke-dasharray: 5 5;

    %% Class Assignments
    class DL3,DT3,PL3,PT3 s3_folder;
    class DLK,DB s3_bucket;
    class ITP,PIP,PTP,LPIP process;
    class DTP,SQLM,ETLI,LI multi_process;
    class DDB,PADB storage;
    class GHCRDB registry;
    class AD,AR,AI trigger;
