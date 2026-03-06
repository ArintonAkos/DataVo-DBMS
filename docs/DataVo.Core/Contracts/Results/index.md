# Results Overview
The `Results` module specifies the standardized data payloads returned by the DataVo.Core query `Evaluator` and executable Actions. It strictly enforces how execution outcomes—whether datasets or simple success messages—are structured before being serialized to a client or Server frontend.

## Core Responsibilities
* **Standardized Output:** Represents the universal return container for query outcomes.
* **Metadata Encapsulation:** Stores not just raw records but execution telemetry like total rows affected, column definitions, and status tokens.

## Component Breakdown

| Component (File) | Architectural Role |
|------------------|--------------------|
| `QueryResult.cs` | Contains the schema definitions (Column names and types), an ordered list of records (`Dictionary<string, dynamic>`), and operational meta-messages evaluated during execution. |

## Dependencies & Interactions
This module is referenced across `IDbAction` implementations dictating the required output of `PerformAction`. The frontend/networking module will typically convert this strongly-typed `QueryResult` into JSON buffers sent via TCP wrappers.

## Implementation Specifics
* **Supported Capabilities:** Agnostic row projection (handling simple `SELECT *` as well as complex aggregated/joined representations) and structured error mapping.
