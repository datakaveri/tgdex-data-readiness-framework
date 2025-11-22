# Data Flow Diagrams

## 1. High-Level Overview
This diagram shows the general flow from entry points to final outputs.

```mermaid
graph TD
    subgraph Input
        Lambda[Lambda Handler]
        Local[Local Execution]
    end

    subgraph Processing
        S_Main[Structured Main]
        U_Main[Unstructured Main]
    end

    subgraph Output
        JSON[JSON Reports]
        PDF[PDF Report]
        API[CAT API Update]
    end

    Lambda -->|Structured| S_Main
    Lambda -->|Unstructured| U_Main
    Local -->|Structured| S_Main
    Local -->|Unstructured| U_Main

    S_Main --> JSON
    S_Main --> PDF
    S_Main --> API

    U_Main --> JSON
    U_Main --> PDF
    U_Main --> API
```

## 2. Structured Data Flow Detail
Detailed flow within the Structured Data processing module.

```mermaid
graph TD
    Start([Start: structured_main.py]) --> Load[Load Data: input_handler]
    Load --> Infer[Infer Column Roles: llm_api]
    Infer --> Raw[Generate Raw Report: aggregate_structured]
    
    subgraph Metrics Calculation
        Raw --> M1[Check Missing/Duplicates]
        Raw --> M2[Check Variance/Coverage]
        Raw --> M3[Check Formats/Encoding]
    end

    M1 --> Score[Compute Aggregate Score: scoring_structured]
    M2 --> Score
    M3 --> Score
    
    Score --> Write[Write Outputs: json_writer]
    Write --> PDF[Generate PDF: pdf_writer]
    PDF --> End([End])
```

## 3. Unstructured Data Flow Detail
Detailed flow within the Unstructured Data processing module.

```mermaid
graph TD
    Start([Start: unstructured_main.py]) --> Meta[Extract Metadata: metadata_parser]
    Meta --> Infer[Infer Roles: llm_api]
    Infer --> Raw[Generate Raw Report: aggregate_unstructured]

    subgraph Metrics Calculation
        Raw --> M1[Check File Duplicates/Types]
        Raw --> M2[Check Openability/Format]
        Raw --> M3[Check Metadata Coverage]
    end

    M1 --> Score[Compute Aggregate Score: scoring_unstructured]
    M2 --> Score
    M3 --> Score

    Score --> Write[Write Outputs: json_writer]
    Write --> PDF[Generate PDF: pdf_writer]
    PDF --> End([End])
```
